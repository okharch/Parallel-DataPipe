package Parallel::DataPipe;

our $VERSION='0.04';
use 5.008; # Perl::MinimumVersion says that

use strict;
use warnings;
use IO::Select;
use List::Util qw(first max min);
use constant PIPE_MAX_CHUNK_SIZE => ($^O eq 'MSWin32'?1024:16*1024);
use constant _EOF_ => (-(1<<31));

# input_iterator is either array or subroutine reference which puts data into conveyor    
# convert it to sub ref anyway to simplify conveyor loop
sub _get_input_iterator {
    my $input_iterator = shift;
    die "input_iterator is required parameter" unless defined($input_iterator);
    unless (ref($input_iterator) eq 'CODE') {
        die "array or code reference expected for input_iterator" unless ref($input_iterator) eq 'ARRAY';
        my $array = $input_iterator;
        my $l = @$array;
        my $i = 0;
        $input_iterator = sub {$i < $l? $array->[$i++]:undef};
    }
    return $input_iterator;
}


sub run {
    my $param = shift;
    my $input_iterator = _get_input_iterator(delete $param->{'input_iterator'});
    my $conveyor = Parallel::DataPipe->new($param);
    $SIG{ALRM} = 'IGNORE';
    # data processing conveyor. 
    while (defined(my $data = $input_iterator->())) {
        $conveyor->process_data($data);
    }    
    # receive and merge remaining data from busy processors
    my $busy_processors = $conveyor->busy_processors;
    $conveyor->receive_and_merge_data() while $busy_processors--;
}

# this should work with Windows NT or if user explicitly set that
my $number_of_cpu_cores = $ENV{NUMBER_OF_PROCESSORS}; 
sub number_of_cpu_cores {
    #$number_of_cpu_cores = $_[0] if @_; # setter
    return $number_of_cpu_cores if $number_of_cpu_cores;
    eval {
        # try unix (linux,cygwin,etc.)
        $number_of_cpu_cores = scalar grep m{^processor\t:\s\d+\s*$},`cat /proc/cpuinfo 2>/dev/null`;
        # try bsd
        ($number_of_cpu_cores) = map m{hw.ncpu:\s+(\d+)},`sysctl -a` unless $number_of_cpu_cores;
    };
    # otherwise it sets number_of_cpu_cores to 2
    return $number_of_cpu_cores || 1;
}

sub freeze {
	my $self = shift;
	$self->{freeze}->(@_);
}

sub thaw {
	my $self = shift;
	$self->{thaw}->(@_);
}

# this inits freeze and thaw with Storable subroutines and try to replace them with Sereal counterparts
sub _init_serializer {
    my ($self,$param) = @_;
    my ($freeze,$thaw) = grep $_ && ref($_) eq 'CODE',map delete $param->{$_},qw(freeze thaw);
    if ($freeze && $thaw) {
        $self->{freeze} = $freeze;
        $self->{thaw} = $thaw;
    } else {
        # try cereal        
        eval q{
            use Sereal qw(encode_sereal decode_sereal);
            $self->{freeze} = \&encode_sereal;
            $self->{thaw} = \&decode_sereal;
            1;
        }
        or
        eval q{
            use Storable;
            $self->{freeze} = \&Storable::nfreeze;
            $self->{thaw} = \&Storable::thaw; 
        };
        
    }
    # don't make any assumptions on serializer capabilities, give all the power to user ;)
    # die "bad serializer!" unless join(",",@{$thaw->($freeze->([1,2,3]))}) eq '1,2,3';
}


# this subroutine reads data from pipe and converts it to perl reference
# or scalar - if size is negative
# it always expects size of frozen scalar so it know how many it should read
# to feed thaw 
sub _get_data {
    my ($self,$fh) = @_;
    my ($data_size,$data,$process_num);
    $fh->sysread($data_size,4);
    $data_size = unpack("l",$data_size);
    return undef if $data_size == _EOF_; # this if for process_data terminating
    if ($data_size == 0) {
        $data = '';
    } else {
        my $length = abs($data_size);
        my $offset = 0;
        # allocate all the buffer for $data beforehand
        $data = sprintf("%${length}s","");
        while ($offset < $length) {
            my $chunk_size = min(PIPE_MAX_CHUNK_SIZE,$length-$offset);        
            $fh->sysread(my $buf,$chunk_size);
            # use lvalue form of substr to copy data in preallocated buffer        
            substr($data,$offset,$chunk_size) = $buf;
            $offset += $chunk_size;
        }
        $data = $self->thaw($data) if $data_size<0; 
    }
    return $data;
}

# this subroutine serialize data reference. otherwise 
# it puts negative size of scalar and scalar itself to pipe.
# parameter $process_num defined means it's child who is writing
# to shared pipe to communicate with parent
sub _put_data {
    my ($self,$fh,$data) = @_;
    unless (defined($data)) {
        $fh->syswrite(pack("l", _EOF_));
        return;        
    }
    my $length = length($data);
    if (ref($data)) {
        $data = $self->freeze($data);
        $length = -length($data);
    }
    $fh->syswrite(pack("l", $length));
    $length = abs($length);
    my $offset = 0;
    while ($offset < $length) {
        my $chunk_size = min(PIPE_MAX_CHUNK_SIZE,$length-$offset);        
        $fh->syswrite(substr($data,$offset,$chunk_size));
        $offset += $chunk_size;
    }
}

sub _fork_data_processor {
    my ($data_processor_callback) = @_;
    # create processor as fork
    local $SIG{TERM} = sub {exit;}; # exit silently from data processors
    my $pid = fork();
    unless (defined $pid) {
        #print "say goodbye - can't fork!\n"; <>;
        die "can't fork!";
    }
    if ($pid == 0) {
        # data processor is eternal loop which wait for raw data on pipe from main
        # data processor is killed when it's not needed anymore by _kill_data_processors
        $data_processor_callback->() while (1);
        exit;
    }
    return $pid;
}

sub _create_data_processor {
    my ($self,$process_data_callback,$process_num) = @_;
    
    # parent <=> child pipes
    my ($parent_read, $parent_write) = pipely();
    my ($child_read, $child_write) = pipely();
 
    my $data_processor = sub {
        local $_ = $self->_get_data($child_read);
        exit 0 unless defined($_);
        $_ = $process_data_callback->($_);
        $self->_put_data($parent_write,$_);
    };
    
    # return data processor record 
    return {
        pid => _fork_data_processor($data_processor),  # needed to kill processor when there is no more data to process
        child_write => $child_write,                 # pipe to write raw data from main to data processor 
        parent_read => $parent_read,                 # pipe to write raw data from main to data processor 
    };
}

sub _create_data_processors {
    my ($self,$process_data_callback,$number_of_data_processors) = @_;
    $number_of_data_processors = $self->number_of_cpu_cores unless $number_of_data_processors;
    die "process_data parameter should be code ref" unless ref($process_data_callback) eq 'CODE';
	die "\$number_of_data_processors:undefined" unless defined($number_of_data_processors);
    return [map $self->_create_data_processor($process_data_callback,$_), 0..$number_of_data_processors-1];
}

sub process_data {
	my ($self,$data) = @_;
    my $processor;
    if ($self->{free_processors}) {
        $processor = $self->{processors}[--$self->{free_processors}];
    } else {
        # wait for some processor which processed data
        $processor = $self->receive_and_merge_data;
    }
    $processor->{item_number} = $self->{item_number}++;
    die "no support of data processing for undef items!" unless defined($data);
    $self->_put_data($processor->{child_write},$data);
}

# this method is called once when input data is ended
# to find out how many processors were involved in data processing
# then it calls receive_and_merge_data 1..busy_processors times
sub busy_processors {
    my $self = shift;
    return @{$self->{processors}} - $self->{free_processors};
}

sub receive_and_merge_data {
	my $self = shift;
    my ($processors,$data_merge_code,$ready) = @{$self}{qw(processors data_merge_code ready)};
    $self->{ready} = $ready = [] unless $ready;
    @$ready = IO::Select->new(map $_->{parent_read},@$processors)->can_read() unless @$ready;
    my $fh = shift(@$ready);
    my $processor = first {$_->{parent_read} == $fh} @$processors;
    local $_ = $self->_get_data($fh);
    $data_merge_code->($_,$processor->{item_number});
    return $processor;
}
    
sub _kill_data_processors {
    my ($self) = @_;
    my $processors = $self->{processors};
    my @pid_to_kill = map $_->{pid}, @$processors;
    my %pid_to_wait = map {$_=>undef} @pid_to_kill;
    # put undef to input of data_processor - they know it's time to exit
    $self->_put_data($_->{child_write}) for @$processors;
    while (keys %pid_to_wait) {
        my $pid = wait;
        last if $pid == -1;
        delete $pid_to_wait{$pid};
    }
}

sub new {
    my ($class, $param) = @_;	
	my $self = {};
    bless $self,$class;    
    # item_number for merge implementation
    $self->{item_number} = 0;    
    # check if user want to use alternative serialisation routines
    $self->_init_serializer($param);    
    # @$processors is array with data processor info
    $self->{processors} = $self->_create_data_processors(
        map delete $param->{$_},qw(process_data number_of_data_processors)
    );
    # this counts processors which are still not involved in processing data
    $self->{free_processors} = @{$self->{processors}};    
    # data_merge is sub which merge all processed data inside parent thread
    # it is called each time after process_data returns some new portion of data    
    $self->{data_merge_code} = delete $param->{'merge_data'};
    die "data_merge should be code ref" unless ref($self->{data_merge_code}) eq 'CODE';    
    my $not_supported = join ", ", keys %$param;
    die "Parameters are not supported:". $not_supported if $not_supported;	
	return $self;
}

sub DESTROY {
	my $self = shift;
    $self->_kill_data_processors;
    #semctl($self->{sem_id},0,IPC_RMID,0);
}

=comment Why I copied IO::Pipely::pipely instead of use IO::Pipely qw(pipely)?
1. Do not depend on installation of additional module
2. I don't know (yet) how to win race condition:
A) In Makefile.PL I would to check if fork & pipe works on the platform before creating Makefile.
But I am not sure if it's ok that at that moment I can use pipely to create pipes.
so
B) to use pipely I have to create makefile
For now I decided just copy code for pipely into this module.
Then if I know how do win that race condition I will get rid of this code and
will use IO::Pipely qw(pipely) instead and
will add dependency on it.
=cut
# IO::Pipely is copyright 2000-2012 by Rocco Caputo.
use Symbol qw(gensym);
use IO::Socket qw(
  AF_UNIX
  PF_INET
  PF_UNSPEC
  SOCK_STREAM
  SOL_SOCKET
  SOMAXCONN
  SO_ERROR
  SO_REUSEADDR
  inet_aton
  pack_sockaddr_in
  unpack_sockaddr_in
);
use Fcntl qw(F_GETFL F_SETFL O_NONBLOCK);
use Errno qw(EINPROGRESS EWOULDBLOCK);

my (@oneway_pipe_types, @twoway_pipe_types);
if ($^O eq "MSWin32" or $^O eq "MacOS") {
  @oneway_pipe_types = qw(inet socketpair pipe);
  @twoway_pipe_types = qw(inet socketpair pipe);
}
elsif ($^O eq "cygwin") {
  @oneway_pipe_types = qw(pipe inet socketpair);
  @twoway_pipe_types = qw(inet pipe socketpair);
}
else {
  @oneway_pipe_types = qw(pipe socketpair inet);
  @twoway_pipe_types = qw(socketpair inet pipe);
}

sub pipely {
  my %arg = @_;

  my $conduit_type = delete($arg{type});
  my $debug        = delete($arg{debug}) || 0;

  # Generate symbols to be used as filehandles for the pipe's ends.
  #
  # Filehandle autovivification isn't used for portability with older
  # versions of Perl.

  my ($a_read, $b_write)  = (gensym(), gensym());

  # Try the specified conduit type only.  No fallback.

  if (defined $conduit_type) {
    return ($a_read, $b_write) if _try_oneway_type(
      $conduit_type, $debug, \$a_read, \$b_write
    );
  }

  # Otherwise try all available conduit types until one works.
  # Conduit types that fail are discarded for speed.

  while (my $try_type = $oneway_pipe_types[0]) {
    return ($a_read, $b_write) if _try_oneway_type(
      $try_type, $debug, \$a_read, \$b_write
    );
    shift @oneway_pipe_types;
  }

  # There's no conduit type left.  Bummer!

  $debug and warn "nothing worked";
  return;
}

# Try a pipe by type.

sub _try_oneway_type {
  my ($type, $debug, $a_read, $b_write) = @_;

  # Try a pipe().
  if ($type eq "pipe") {
    eval {
      pipe($$a_read, $$b_write) or die "pipe failed: $!";
    };

    # Pipe failed.
    if (length $@) {
      warn "pipe failed: $@" if $debug;
      return;
    }

    $debug and do {
      warn "using a pipe";
      warn "ar($$a_read) bw($$b_write)\n";
    };

    # Turn off buffering.  POE::Kernel does this for us, but
    # someone might want to use the pipe class elsewhere.
    select((select($$b_write), $| = 1)[0]);
    return 1;
  }

  # Try a UNIX-domain socketpair.
  if ($type eq "socketpair") {
    eval {
      socketpair($$a_read, $$b_write, AF_UNIX, SOCK_STREAM, PF_UNSPEC)
        or die "socketpair failed: $!";
    };

    if (length $@) {
      warn "socketpair failed: $@" if $debug;
      return;
    }

    $debug and do {
      warn "using a UNIX domain socketpair";
      warn "ar($$a_read) bw($$b_write)\n";
    };

    # It's one-way, so shut down the unused directions.
    shutdown($$a_read,  1);
    shutdown($$b_write, 0);

    # Turn off buffering.  POE::Kernel does this for us, but someone
    # might want to use the pipe class elsewhere.
    select((select($$b_write), $| = 1)[0]);
    return 1;
  }

  # Try a pair of plain INET sockets.
  if ($type eq "inet") {
    eval {
      ($$a_read, $$b_write) = _make_socket();
    };

    if (length $@) {
      warn "make_socket failed: $@" if $debug;
      return;
    }

    $debug and do {
      warn "using a plain INET socket";
      warn "ar($$a_read) bw($$b_write)\n";
    };

    # It's one-way, so shut down the unused directions.
    shutdown($$a_read,  1);
    shutdown($$b_write, 0);

    # Turn off buffering.  POE::Kernel does this for us, but someone
    # might want to use the pipe class elsewhere.
    select((select($$b_write), $| = 1)[0]);
    return 1;
  }

  # There's nothing left to try.
  $debug and warn "unknown pipely() socket type ``$type''";
  return;
}


1;

=head1 NAME

C<Parallel::DataPipe> - parallel data processing conveyor 

=encoding utf-8

=head1 SYNOPSIS

    use Parallel::DataPipe;
    Parallel::DataPipe::run {
        input_iterator => [1..100],
        process_data => sub { "$_:$$" },
        number_of_data_processors => 100,
        merge_data => sub { print "$_\n" },
    };
    

=head1 DESCRIPTION


If you have some long running script processing data item by item
(having on input some data and having on output some processed data i.e. aggregation, webcrawling,etc)
you can speed it up 4-20 times using parallel datapipe conveyour.
Modern computer (even modern smartphones ;) ) have multiple CPU cores: 2,4,8, even 24!
And huge amount of memory: memory is cheap now.
So they are ready for parallel data processing.
With this script there is an easy and flexible way to use that power.

So what are the benefits of this module?

1) because it uses input_iterator it does not have to know all input data before starting parallel processing

2) because it uses merge_data processed data is ready for using in main thread immediately.

1) and 2) remove requirements for memory which is needed to store data items before and after parallel work. and allows parallelize work on collecting, processing and using processed data.

If you don't want to overload your database with multiple simultaneous queries
you make queries only within input_iterator and then process_data and then flush it with merge_data.
On the other hand you usually win if make queries in process_data and do a lot of data processors.
Possibly even more then physical cores if database queries takes a long time and then small amount to process.

It's not surprise, that DB servers usually serves N queries simultaneously faster then N queries one by one.

Make tests and you will know.

To (re)write your script for using all processing power of your server you have to find out:

1) the method to obtain source/input data. I call it input iterator. It can be either array with some identifiers/urls or reference to subroutine which returns next portion of data or undef if there is nor more data to process.

2) how to process data i.e. method which receives input item and produce output item. I call it process_data subroutine. The good news is that item which is processed and then returned can be any scalar value in perl, including references to array and hashes. It can be everything that Storable can freeze and then thaw.

3) how to use processed data. I call it merge_data. In the example above it just prints an item, but you could do buffered inserts to database, send email, etc.

Take into account that 1) and 3) is executed in main script thread. While all 2) work is done in parallel forked threads. So for 1) and 3) it's better not to do things that block execution and remains hungry dogs 2) without meat to eat. So (still) this approach will benefit if you know that bottleneck in you script is CPU on processing step. Of course it's not the case for some web crawling tasks unless you do some heavy calculations

=head2 run

This is subroutine which covers magic of parallelizing data processing.
It receives paramaters with these keys via hash ref.

B<input_iterator> - reference to array or subroutine which should return data item to be processed.
    in case of subroutine it should return undef to signal EOF.

B<process_data> - reference to subroutine which process data items. they are passed via $_ variable
	Then it should return processed data. this subroutine is executed in forked process so don't
    use any shared resources inside it.
    Also you can update children state, but it will not affect parent state.

B<merge_data> - reference to a subroutine which receives data item which was processed  in $_ and now going to be merged
	this subroutine is executed in parent thread, so you can rely on changes that it made after C<process_data> completion.

These parameters are optional and has reasonable defaults, so you change them only know what you do

B<number_of_data_processors> - (optional) number of parallel data processors. if you don't specify,
    it tries to find out a number of cpu cores
	and create the same number of data processor children.
    It looks for NUMBER_OF_PROCESSORS environment variable, which is set under Windows NT.
    If this environment variable is not found it looks to /proc/cpuinfo which is availbale under Unix env.
    It makes sense to have explicit C<number_of_data_processors>
    which possibly is greater then cpu cores number
    if you are to use all slave DB servers in your environment 
    and making query to DB servers takes more time then processing returned data.
    Otherwise it's optimal to have C<number_of_data_processors> equal to number of cpu cores.

B<freeze>, B<thaw> - you can use alternative serializer. 
    for example if you know that you are working with array of words (0..65535) you can use
    freeze => sub {pack('S*',@{$_[0]})} and thaw => sub {[unpack('S*',$_[0])]}
    which will reduce the amount of bytes exchanged between processes.
    But do it as the last optimization resort only.
    In fact automatic choise is quite good and efficient.
    It uses encode_sereal and decode_sereal if Sereal module is found.
    Otherwise it use Storable freeze and thaw.

=head2 HOW IT WORKS

1) Main thread (parent) forks C<number_of_data_processors> of children for processing data.

2) As soon as data comes from C<input_iterator> it sends it to next child using
pipe mechanizm.

3) Child processes data and returns result back to parent using pipe.

4) Parent firstly fills up all the pipes to children with data and then
starts to expect processed data on pipes from children.

5) If it receives result from chidlren it sends processed data to C<data_merge> subroutine,
and starts loop 2) again.

6) loop 2) continues until input data is ended (end of C<input_iterator> array or C<input_iterator> sub returned undef).

7) In the end parent expects processed data from all busy chidlren and puts processed data to C<data_merge>

8) After having all the children sent processed data they are killed and run returns to the caller.

Note:
 If C<input_iterator> or <process_data> returns reference, it serialize/deserialize data before/after pipe.
 That way you have full control whether data will be serialized on IPC.
 
=head1 SEE ALSO

L<fork|http://perldoc.perl.org/functions/fork.html>

L<subs::parallel>

L<Parallel::Loops>

L<MCE>

L<Parallel::parallel_map>

=head1 DEPENDENCIES

These should all be in perl's core:

 use Storable qw(freeze thaw);
 use IO::Select;
 use POSIX ":sys_wait_h";


For tests:

 use Test::More tests => 21;
 use Time::HiRes qw(time);


if found it uses Sereal module for serialization instead of Storable as the former is more efficient.

=head1 BUGS 

For all bugs small and big and also what do you think about this stuff please send email to okharch@gmail.com.

=head1 SOURCE REPOSITORY

See the git source on github
 L<https://github.com/okharch/Parallel-DataPipe>

=head1 COPYRIGHT

Copyright (c) 2013 Oleksandr Kharchenko <okharch@gmail.com>

All right reserved. This library is free software; you can redistribute it
and/or modify it under the same terms as Perl itself.

=head1 AUTHOR

  Oleksandr Kharchenko <okharch@gmail.com>

=cut
