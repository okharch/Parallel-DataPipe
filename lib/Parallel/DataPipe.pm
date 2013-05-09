package Parallel::DataPipe;

our $VERSION='0.01';
use 5.008; # Perl::MinimumVersion says that

use strict;
use warnings;
use Storable qw(freeze thaw);
use IO::Select;
use POSIX ":sys_wait_h";
use constant _EOF_ => (-(2 << 31)+1);

{
    # this works correct only in unix/linux environment. cygwin as well.
    # otherwise it sets processors_number to 2
	my $processors_number;
	sub processors_number {
		$processors_number = scalar grep m{^processor\t:\s\d+\s*$},`cat /proc/cpuinfo|grep processor` unless $processors_number;
        $processors_number = 2 unless $processors_number; # minimum rational value for this module
		return $processors_number;
	}
} # end of processors_number block

{ # begin of serializer block
my $freeze;
my $thaw;

# this inits freeze and thaw with Storable subroutines and try to replace them with Sereal counterparts
sub _init_serializer {
    my $param = shift;
    if (exists $param->{freeze} && ref($param->{freeze}) eq 'CODE' &&  exists $param->{thaw} && ref($param->{thaw}) eq 'CODE') {
        $freeze = $param->{freeze};
        $thaw = $param->{thaw};
    } else {
        $freeze = \&freeze;
        $thaw = \&thaw;
        # try cereal 
        eval q{
            use Sereal qw(encode_sereal decode_sereal);
            $freeze = \&encode_sereal;
            $thaw = \&decode_sereal;
        };
    }
    # make some simple test with serializer
    die "bad serializer!" unless join(",",@{$thaw->($freeze->([1,2,3]))}) eq '1,2,3';
}


# this subroutine reads data from pipe and converts it to perl reference
# it always expects size of frozen scalar so it know how many it should read
# to feed thaw 
sub _get_data { my ($fh) = @_;    
    my ($data_size,$data);
    read $fh,$data_size,4;
    $data_size = unpack("l",$data_size);
    return undef if ($data_size == _EOF_);
    return  "" if $data_size == 0;
    read $fh,$data,abs($data_size);
    $data = $thaw->($data) if $data_size>0;
    return $data;
}

# this subroutine serialize data reference. otherwise 
# it puts negative size of scalar and scalar itself to pipe.
# it also makes flush to avoid buffering blocks execution
sub _put_data { my ($fh,$data) = @_;
    if (!defined($data)) {
        print $fh pack("l",_EOF_);
    } elsif (ref($data)) {
        $data = $freeze->($data);
        print $fh pack("l", length($data)).$data;
    } else {
        print $fh pack("l",-length($data)).$data;        
    }
    $fh->flush();
}

} # end of serializer block

sub _create_data_processors {
    my ($process_data,$processor_number) = @_;
    die "process_data parameter should be code ref" unless ref($process_data) eq 'CODE';
    my @processors;
    # each processor has these fields:
    # pid - needed to kill processor when there is no more data to process
    # raw_wh - pipe to write raw data for processing from main thread to processor
    # processed_rh  - pipe to read processed data from processor to main thread
    # free - flag whether processor is free to process data (waits for data on raw_rh pipe)
    # data_processor - sub ref for debug purposes
    
    for my $i (1..$processor_number)  {
        # row data pipe main => processor
        pipe(my $raw_rh,my $raw_wh);
        
        # processed data pipe processor => main
        pipe(my $processed_rh,my $processed_wh);
        
        # make closure for single-thread debug purposes
        my $data_processor = sub {
            local $_ = _get_data($raw_rh);
            # process data with given subroutine
            $_ = $process_data->();
            # puts processed data back on pipe to main
            _put_data($processed_wh,$_);
        };
        my $pid;
        unless ($processor_number == 1) {
            # create processor as fork
            $pid = fork();
            unless (defined $pid) {
                #print "say goodbye - can't fork!\n"; <>;
                die "can't fork($i)!";
            }
            if ($pid == 0) {
                # processor is eternal loop which wait for raw data on pipe from main
                # processor is killed when it's not needed anymore by kill_data_processors
                while (1) {
                    $data_processor->();
                }
                exit;
            }
        }
        push @processors,{pid => $pid, raw_wh => $raw_wh, processed_rh => $processed_rh, free => 1,data_processor => $data_processor};
    }    
    return \@processors;
}

sub _process_data {
    my ($data,$processors) = @_;
    my @free_processors = grep $_->{free},@$processors;
    return 1 unless @free_processors;
    my $processor = shift(@free_processors);
    _put_data($processor->{raw_wh},$data);
    if (@$processors == 1) {
        # debug (limited, be careful) purposes
        # execute data processor in current thread
        $processors->{data_processor}->(); 
    } else {
        $processor->{free} = 0; # now it's busy
    }
    return 0;
}

sub _receive_and_merge_data {
    my ($processors,$data_merge_code) = @_;
    
    my @busy_processors = grep $_->{free}==0,@$processors;
    
    # returns false if there is no busy processors
    return 0 unless @busy_processors;
    
    # blocking read until at least one handle is ready
    my @processed_rh = IO::Select->new(map $_->{processed_rh},@busy_processors)->can_read();
    for my $rh (@processed_rh) {
        local $_ = _get_data($rh);
        #print "merge data:$_\n";
        $data_merge_code->();
        $_->{free} = 1 for grep $_->{processed_rh} == $rh, @busy_processors;
    }
    
    # returns true if there was busy processors 
    return 1;        
}    
    
sub _kill_data_processors {
    my ($processors) = @_;
    my @pid_to_kill = map $_->{pid},@$processors;
    my %pid_to_wait = map {$_=>undef} @pid_to_kill;
    kill(1,@pid_to_kill);
    while (keys %pid_to_wait) {
        my $pid = wait;
        last if $pid == -1;
        delete $pid_to_wait{$pid};
    }
}

sub _get_input_iterator {
    my $input_iterator = shift;
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
    
    # check if user want to use alternative serialisation routines
    _init_serializer($param);
    
    # $input_iterator is either array or subroutine reference which puts data into conveyor    
    # convert it to sub anyway
    my $input_iterator = _get_input_iterator($param->{'input_iterator'});
    
    # @$processors is array with data processor info (see also _create_data_processors)    
    my $processors = _create_data_processors($param->{'process_data'},$param->{'processor_number'} || processors_number); #[pid,$send_wh,$receive_rh,$free]
    
    # data_merge is sub which merge all processed data inside parent thread
    # it is called each time after process_data returns some new portion of data    
    my $data_merge_code = $param->{'merge_data'};
    die "data_merge should be code ref" unless ref($data_merge_code) eq 'CODE';
    
    # data process conveyor. 
    while (my $data = $input_iterator->()) {
        my $wait = _process_data($data,$processors);
        if ($wait) {
            _receive_and_merge_data($processors,$data_merge_code);
            _process_data($data,$processors);
        }
    }
    
    # receive and merge remaining data from parallel processors
    while (_receive_and_merge_data($processors,$data_merge_code)) {}

    # now kill & rip all data processors
    _kill_data_processors($processors);
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
        processor_number => 100,
        merge_data => sub { print "$_\n" },
    };
    

=head1 DESCRIPTION

If you have some long running script processing data item by item
(having on input some data and having on output some processed data i.e. aggregation, webcrawling,etc)
here is good news for you:

You can speed it up 4-20 times with minimal efforts from you.
Modern computer (even modern smartphones ;) ) have multiple CPU cores: 2,4,8, even 24!
And huge amount of memory: memory is cheap now.
So they are ready for parallel data processing.
With this script there is easy and flexible way to use that power.

Well, it is not first method on parallelizm in Perl.
You could write efficient crawler using single core and framework like Coro::LWP or AnyEvent::HTTP::LWP.
Also you can elegantly use all your cores for parallel processing using Parallel::Loop.
So what are benefits of this module?

1) because it uses input_iterator it does not have to know all input data before starting parallel processing

2) because it uses merge_data processed data is ready for using in main thread immediately.

1) and 2) remove requirements for memory which is needed to store data items before and after parallel work.
and allows parallelize work on collecting, processing and using processed data.

Usually if you don't want to overload your database with multiple simultaneous connections
you do can do queries only by input_iterator and then process_data
and then flush it with merge_data.

To (re)write your script for using all processing power of your server you have to find out:

1) the method to obtain source/input data.
I call it input iterator.
It can be either array with some identifiers/urls or reference to subroutine which returns next portion of data or undef if there is nor more data to process.

2) how to process data i.e. method which receives input item and produce output item.
I call it process_data subroutine.
The good news is that item which is processed and then returned
can be any scalar value in perl, including references to array and hashes.
It can be everything that Storable can freeze and then thaw.

3) how to use processed data. I call it merge_data.
In the example above it just prints an item, but you could do buffered inserts to database, send email, etc.

Take into account that 1) and 3) is executed in main script thread. While all 2) work is done in parallel forked threads.
So for 1) and 3) it's better not to do things that block execution and remains hungry dogs 2) without meat to eat.
So (still) this approach will benefit if you know that bottleneck in you script is CPU on processing step.
Of course it's not the case for some web crawling tasks unless you do some heavy calculations

P.S. Please help me to improve documentation on this module. I understand my English is far from perfect
and so probably is clarity of explanations. Thank you!

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

B<processor_number> - (optional) number of parallel data processors. if you don't specify,
    it tries to find out a number of cpu cores
	and create the same number of data processor children.
    It makes sense to have explicit C<processor_number>
    which possibly is greater then cpu cores number
    if you are to use all slave DB servers in your environment 
    and making query to DB servers takes more time then processing returned data.
    Otherwise it's optimal to have C<processor_number> equal to physical number of cores.

B<freeze>, B<thaw> - you can use alternative serializer. 
    for example if you know that you are working with array of words (0..65535) you can use
    freeze => sub {pack('S*',$_[0])} and thaw => sub {unpack('S*',$_[0])}
    which will reduce the amount of bytes exchanged between processes.
    But do it as the last optimization resort only.
    In fact automatic choise is quite good and efficient.
    It uses encode_sereal and decode_sereal if Sereal module is found.
    Otherwise it use Storable freeze and thaw.
=head3 How It Works

1. Main thread (parent) forks C<processor_number> of children for processing data.

2. As soon as data comes from C<input_iterator> it sends it to to next child using
serialization and pipe mechanizm.

3. Child deserialize it, process it, serialize the result and put it to pipe for parent.

4. Parent firstly fills up all the pipes to children with data and then starts to expect processed data on pipes from children.

5. If it receives data from chidlren it sends processed data to C<data_merge> subroutine,
puts new portion of unprocessed data to that childs pipe (step 2).

6. This conveyor works until input data is ended (end of C<input_iterator> array or C<input_iterator> sub returned undef).

7. In the end parent expects processed data from all busy chidlren and puts processed data to C<data_merge>

8. After having all the children sent processed data they are killed and run returns to the caller.


=head1 SEE ALSO

L<fork|http://perldoc.perl.org/functions/fork.html>

L<subs::parallel>

L<Parallel::Loops>

=head1 DEPENDENCIES

These should all be in perl's core:

 use Storable qw(freeze thaw);
 use IO::Select;
 use POSIX ":sys_wait_h";


For tests:

 use Test::More tests => 21;
 use Time::HiRes qw(time);


if found it uses Sereal module for serialization instead of Storable as it is more efficient.

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
