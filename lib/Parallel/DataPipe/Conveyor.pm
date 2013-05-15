# this basic implementation should work ok for all unix flavors: linux, solaris, bsd, cygwin
package Parallel::DataPipe::Conveyor;

our $VERSION='0.03';
use 5.008; # Perl::MinimumVersion says that

use strict;
use warnings;
use Storable qw(freeze thaw);
use IO::Select;
use POSIX ":sys_wait_h";
use constant _EOF_ => (-(2 << 31)+1);
use Carp qw(confess);

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
    if (exists $param->{freeze} && ref($param->{freeze}) eq 'CODE' &&  exists $param->{thaw} && ref($param->{thaw}) eq 'CODE') {
        $self->{freeze} = $param->{freeze};
        $self->{thaw} = $param->{thaw};
    } else {
        $self->{freeze} = \&freeze;
        $self->{thaw} = \&thaw;
        # try cereal 
        eval q{
            use Sereal qw(encode_sereal decode_sereal);
            $self->{freeze} = \&encode_sereal;
            $self->{thaw} = \&decode_sereal;
        };
    }
    # don't make any assumptions on serializer capabilities, give all the power to user ;)
    # die "bad serializer!" unless join(",",@{$thaw->($freeze->([1,2,3]))}) eq '1,2,3';
}


# this subroutine reads data from pipe and converts it to perl reference
# or scalar - if size is negative
# it always expects size of frozen scalar so it know how many it should read
# to feed thaw 
sub _get_data { my ($self,$fh) = @_;    
    my ($data_size,$data);
    read $fh,$data_size,4;
    $data_size = unpack("l",$data_size);
    return undef if ($data_size == _EOF_);
    return  "" if $data_size == 0;
    read $fh,$data,abs($data_size);
    $data = $self->{thaw}->($data) if $data_size>0;
    return $data;
}

# this subroutine serialize data reference. otherwise 
# it puts negative size of scalar and scalar itself to pipe.
# it also makes flush to avoid buffering blocks execution
sub _put_data { my ($self,$fh,$data) = @_;
    if (!defined($data)) {
        print $fh pack("l",_EOF_);
    } elsif (ref($data)) {
        $data = $self->{freeze}->($data);
        print $fh pack("l", length($data)).$data;
    } else {
        print $fh pack("l",-length($data)).$data;        
    }
    $fh->flush();
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
    my ($self,$process_data_callback) = @_;
    # row data pipe main => processor
    pipe(my $read_raw_data_pipe,my $write_raw_data_pipe);
    
    # processed data pipe processor => main
    pipe(my $read_processed_data_pipe,my $write_processed_data_pipe);
    
    my $data_processor = sub {
	# wait for data from raw data pipe
        local $_ = $self->_get_data($read_raw_data_pipe);
        # process data with given subroutine
        $_ = $process_data_callback->($_);
        # puts processed data back on pipe to main
        $self->_put_data($write_processed_data_pipe,$_);
    };
    
    # return data processor record 
    return {
        pid => _fork_data_processor($data_processor),            # needed to kill processor when there is no more data to process
        write_raw_data_pipe => $write_raw_data_pipe,            # pipe to write raw data from main to data processor 
        read_processed_data_pipe => $read_processed_data_pipe,  # pipe to read processed data from processor to main thread                                                                    
        is_free => 1,                                           # flag whether processor is free for processing data
                                                                # (waits for data on read_raw_data_pipe )
    };    
}

sub _create_data_processors {
    my ($self,$process_data_callback,$number_of_data_processors) = @_;
    die "process_data parameter should be code ref" unless ref($process_data_callback) eq 'CODE';
	confess "\$number_of_data_processors:undefined" unless defined($number_of_data_processors);
    return [map $self->_create_data_processor($process_data_callback), 1..$number_of_data_processors];
}

sub process_data {
	my ($self,$data) = @_;
    my $processors = $self->{processors};
    my @free_processors = grep $_->{is_free},@$processors;
    return 1 unless @free_processors;
    my $processor = shift(@free_processors);
    $self->_put_data($processor->{write_raw_data_pipe},$data);
    $processor->{is_free} = 0; # now it's busy
    return 0; 
}

sub receive_and_merge_data {
	my $self = shift;
    my ($processors,$data_merge_code) = @{$self}{qw(processors data_merge_code)};
    
    my @busy_processors = grep $_->{is_free}==0,@$processors;
    
    # returns false if there is no busy processors
    return 0 unless @busy_processors;
    
    # blocking read until at least one handle is ready
    my @read_processed_data_pipe = IO::Select->new(map $_->{read_processed_data_pipe},@busy_processors)->can_read();
    for my $rh (@read_processed_data_pipe) {
        local $_ = $self->_get_data($rh);
        $data_merge_code->($_);
        $_->{is_free} = 1 for grep $_->{read_processed_data_pipe} == $rh, @busy_processors;
    }
    
    # returns true if there was busy processors 
    return 1;        
}
    
sub _kill_data_processors {
    my ($processors) = @_;
    my @pid_to_kill = map $_->{pid}, @$processors;
    my %pid_to_wait = map {$_=>undef} @pid_to_kill;
    kill('SIGTERM',@pid_to_kill);
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
	
    # check if user want to use alternative serialisation routines
    $self->_init_serializer($param);    

    # @$processors is array with data processor info
    $self->{processors} = $self->_create_data_processors($param->{'process_data'},$param->{'number_of_data_processors'} || $class->number_of_cpu_cores); 
    
    # data_merge is sub which merge all processed data inside parent thread
    # it is called each time after process_data returns some new portion of data    
    $self->{data_merge_code} = $param->{'merge_data'};
    die "data_merge should be code ref" unless ref($self->{data_merge_code}) eq 'CODE';
	bless $self, $class;
	return $self;
}

sub DESTROY {
	my $self = shift;
    _kill_data_processors($self->{processors});	
}

1;
