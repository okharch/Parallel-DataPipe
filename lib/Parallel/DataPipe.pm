package Parallel::DataPipe;

our $VERSION='0.01';
use 5.008; # Perl::MinimumVersion says that

use strict;
use warnings;
use Storable qw(freeze thaw);
use IO::Select;
use POSIX ":sys_wait_h";
use constant _EOF_ => (-(2 << 31)+1);
use Carp;

{
    # this works ok only in unix/linux environment. cygwin as well.
	my $processors_number;
	sub processors_number {
		# linux/cygwin only 
		$processors_number = scalar grep m{^processor\t:\s\d+\s*$},`cat /proc/cpuinfo|grep processor` unless $processors_number;
		return $processors_number;
	}
} # end of processors_number block

my $freeze;
my $thaw;

# this inits freeze and thaw with Storable subroutines and try to replace them with Sereal counterparts
sub _init_serializer {
    $freeze = \&freeze;
    $thaw = \&thaw;    
    # try cereal 
    eval q{
        use Sereal qw(encode_sereal decode_sereal);
        $freeze = \&encode_sereal;
        $thaw = \&decode_sereal;
    };
}

_init_serializer;

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

sub _create_data_processors {
    my ($process_data,$processor_number) = @_;
    my @processors;
    # each processor has these fields:
    # pid - needed to kill processor when there is no more data to process
    # raw_wh - pipe to write raw data for processing from main thread to processor
    # processed_rh  - pipe to read processed data from processor to main thread
    # free - flag whether processor is free to process data (waits for data on raw_rh pipe)
    # data_processor - sub ref fo debug purposes
    
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
                confess "can't fork($i)!";
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
        confess "array or code reference expected for input_iterator iterator" unless ref($input_iterator) eq 'ARRAY';
        my $array = $input_iterator;
        my $l = @$array;
        my $i = 0;
        $input_iterator = sub {$i < $l? $array->[$i++]:undef};
    }
    return $input_iterator;
}

sub run {
    my $param = shift;
    
    # $input_iterator is either array or subroutine reference which puts data into conveyor    
    # convert it to sub anyway
    my $input_data_get = _get_input_iterator($param->{'input_iterator'});
    
    # @$processors is array with data processor info (see also _create_data_processors)    
    my $processors = _create_data_processors($param->{'process_data'},$param->{'processor_number'} || processors_number); #[pid,$send_wh,$receive_rh,$free]
    
    # data_merge is sub which merge all processed data inside parent thread
    # it is called each time after process_data returns some new portion of data    
    my $data_merge_code = $param->{'merge_data'};
    
    # data process conveyor. 
    while (my $data = $input_data_get->()) {
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

Parallel::DataPipe - parallel data processing conveyor 

=encoding utf-8

=head1 SYNOPSIS

    use Parallel::DataPipe;
    my @data = 1..20;
    Parallel::DataPipe::run {
        input_iterator => [1..100],
        process_data => sub { "$_:$$" },
        processor_number => 100,
        merge_data => sub { print "$_\n" },
    };
    

=head1 DESCRIPTION

With modern multicore computer environment it's crucial to use all processing power to make processing data faster.
This module provide simple way to do that:

1) define how source (input) data is obtained. It could be either array reference or subroutine reference. 

my $conn = DBIx::Connector->new($dsn, $username, $password, {
      RaiseError => 1,
      AutoCommit => 1,
  });
my $sth = $conn->run(fixup => sub {
      my $sth = $_->prepare('SELECT id FROM parent');
      $sth->execute;
      $sth;
  });
my $input_iterator = sub { $sth->fetch };

2) define data processor using key process_data - how input data is processed.

my $process_data = sub {
	my $parent = $_->[0];
    my $children_records = $conn->run(fixup => sub {
          $_->selectall_arrayref('SELECT age FROM children where parent=?',{},$parent);
      });
	my $children_total_age = sum(map $_->[0], @$children_records);
	return [$parent,$children_total_age];
};

3) define what to do with processed records:

my $merge_data = sub {
	print join(":",@$_)."\n";
};

Parallel::DataPipe::run { input_iterator=>$input_iterator, process_data=>$process_data, merge_data => $merge_data };

This approach wins if you do some complex calculations on large data arrays which took a lot of time due processing complexity
during $process_data part. This part is parallelized, so it's advised not to do anything with shared resources in this part.
Instead if you need showing progress, etc. - do it in merge_data part which is executed in parent thread and has access to all parent state.

=head2 run
This is subroutine which covers magic of parallelizing data processing.
It receives paramaters with these keys via hash ref.

input_iterator - reference to array or subroutine which should return data item to be processed.
    in case of subroutine it should return undef to signal EOF.

process_data - reference to subroutine which process data items. they are passed via $_ variable
	Then it should return processed data. this subroutine is executed in forked process so don't
    use any shared resources inside it.
    Also you can update children state, but it will not affect parent state.

processor_number - (optional) number of parallel data processors. if you don't specify,
    it tries to find out a number of cpu cores
	and create the same number of data processor children.
    It makes sense to have explicit processor_number
    which possibly is greater then cpu cores number
    if you are to use all slave DB servers in your environment 
    and making query to DB servers takes more time then processing returned data.
    Otherwise it's optimal to have processor_number equal to physical number of cores.

merge_data - reference to a subroutine which receives data item which was processed  in $_ and now going to be merged
	this subroutine is executed in parent thread, so you can rely on changes that it made after process_data completion.

=head1 SEE ALSO

L<fork>
L<subs::parallel>
L<Parallel::Loops>
=head1 DEPENDENCIES

These should all be in perl's core:

use Storable qw(freeze thaw);
use IO::Select;
use POSIX ":sys_wait_h";

for tests:
use Test::More tests => 21;
use Time::HiRes qw(time);


if found it uses Sereal module for serialization instead of Storable as it is more efficient.

=head1 BUGS 

No bugs are known at the moment. Send any reports to okharch@gmail.com.

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
