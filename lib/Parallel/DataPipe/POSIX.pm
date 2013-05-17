# this basic implementation should work ok for all unix flavors: linux, solaris, bsd, cygwin
package Parallel::DataPipe::POSIX;

use 5.008; # Perl::MinimumVersion says that

use strict;
use warnings;
use Storable;
use IO::Select;
use POSIX;
use constant _EOF_ => (-(2 << 31)+1);
use Carp qw(confess);
#use IPC::SysV qw(IPC_PRIVATE IPC_CREAT IPC_RMID);
use Thread::Semaphore;
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
        $self->{freeze} = \&Storable::nfreeze;
        $self->{thaw} = \&Storable::thaw;
        return;
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
sub _get_data {
    my ($self,$fh) = @_;
    my $get_process_num = wantarray; # this means it expects process_num
    my ($data_size,$data,$process_num);
    if ($get_process_num) {
        POSIX::read($fh,$process_num,2);
        $process_num = unpack("S*",$process_num);
    }
    POSIX::read($fh,$data_size,4);
    $data_size = unpack("l",$data_size);
    if ($data_size != _EOF_) {
        if ($data_size == 0) {
            $data = '';
        } else {
            POSIX::read($fh,$data,abs($data_size));
            $data = $self->thaw($data) if $data_size>0;            
        }
    }
    debug("got data from %s : %s",($get_process_num?"child $process_num":"parent"),$data);
    return $get_process_num?($process_num,$data):$data;
}

# this subroutine serialize data reference. otherwise 
# it puts negative size of scalar and scalar itself to pipe.
# parameter $process_num defined means it's child who is writing
# to shared pipe to communicate with parent
sub _put_data {
    my ($self,$fh,$data,$process_num) = @_;
    if (defined($process_num)) {
        # pipe is shared, must use semaphore
        debug("child %s semaphore entering %s",$process_num,$data);
        semaphore_up($self->{sem_id});
        debug("child %s semaphore protected %s",$process_num,$data);
        POSIX::write($fh,pack("S*",$process_num),2);
    }
    debug("put data from %s : %s",(defined($process_num)?"child $process_num":"parent"),$data);
    if (!defined($data)) {
        POSIX::write($fh,pack("l",_EOF_),4);
    } elsif (ref($data)) {
        $data = $self->freeze($data);
        # length of serialized data
        POSIX::write($fh, pack("l", length($data)), 4); 
        POSIX::write($fh, $data, length($data));
    } else {
        # (negative) length of scalar
        POSIX::write($fh, pack("l", -length($data)), 4);
        POSIX::write($fh, $data, length($data));
    }
    if (defined($process_num)) {
        debug("child %s semaphore freeing %s",$process_num,$data);
        semaphore_down($self->{sem_id});
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
    
    # row data pipe main => processor
    my ($read, $write) = POSIX::pipe();
 
    my $data_processor = sub {
	# wait for data from raw data pipe
        local $_ = $self->_get_data($read);
        # process data with given subroutine
        $_ = $process_data_callback->($_);
        # puts processed data back on pipe to main
        $self->_put_data($self->{write_pipe},$_,$process_num);
    };
    
    # return data processor record 
    return {
        pid => _fork_data_processor($data_processor),  # needed to kill processor when there is no more data to process
        write_raw_data_pipe => $write,                 # pipe to write raw data from main to data processor 
    };    
}

sub _create_data_processors {
    my ($self,$process_data_callback,$number_of_data_processors) = @_;
    $number_of_data_processors = $self->number_of_cpu_cores unless $number_of_data_processors;
    die "process_data parameter should be code ref" unless ref($process_data_callback) eq 'CODE';
	confess "\$number_of_data_processors:undefined" unless defined($number_of_data_processors);
    return [map $self->_create_data_processor($process_data_callback,$_), 0..$number_of_data_processors-1];
}

sub process_data {
	my ($self,$data) = @_;
    debug('process data:%s',$data);
    my $free_processors = $self->{free_processors};
    my $free_processor;
    if ($free_processors) {
        $free_processor = --$self->{free_processors};
    } else {
        # wait for some which processed date
        $free_processor = $self->receive_and_merge_data;
    }
    my $processor = $self->{processors}[$free_processor];
    $processor->{item_number} = $self->{item_number}++;
    $self->_put_data($processor->{write_raw_data_pipe},$data);
}

# this method is called once when input data EOF.
# to find out how many processors were involved in data processing
# then it calls receive_and_merge_data 1..busy_processors times
sub busy_processors {
    my $self = shift;
    return @{$self->{processors}} - $self->{free_processors};
}

sub receive_and_merge_data {
	my $self = shift;
    my $read_pipe = $self->{read_pipe};
    my ($processors,$data_merge_code) = @{$self}{qw(processors data_merge_code)};
    my $process_num;
    local $_;
    ($process_num,$_) = $self->_get_data($read_pipe);
    my $item_number = $processors->[$process_num]{item_number};
    $data_merge_code->($_,$item_number);
    return $process_num;
}
    
sub _kill_data_processors {
    my ($processors) = @_;
    my @pid_to_kill = map $_->{pid}, @$processors;
    my %pid_to_wait = map {$_=>undef} @pid_to_kill;
    debug('killing data processors : %s',\@pid_to_kill);
    kill('SIGTERM',@pid_to_kill);
    while (keys %pid_to_wait) {
        my $pid = wait;
        last if $pid == -1;
        delete $pid_to_wait{$pid};
        debug('rip child %s',$pid);
    }
    debug('killed & ripped ok');
}

sub new {
    my ($class, $param) = @_;
	
	my $self = {};
    bless $self,$class;
    
    # merge item_number implementation
    $self->{item_number} = 0;
    
    # shared parent pipe. every child uses it to write processed results back to parent
    my ($read, $write) = POSIX::pipe();
    $self->{read_pipe} = $read;
    $self->{write_pipe} = $write;
    
    # semaphore to sync writing to parent pipe
    #$self->{sem_id} = semget(IPC_PRIVATE, 1, 0666 | IPC_CREAT);
    $self->{sem_id} = Thread::Semaphore->new();
	
    # check if user want to use alternative serialisation routines
    $self->_init_serializer($param);    

    # @$processors is array with data processor info
    $self->{processors} = $self->_create_data_processors(
        map delete $param->{$_},qw(process_data number_of_data_processors)
    );
    # this counts processors which are still not involved in processing data
    $self->{free_processors} = @{$self->{processors}};
    debug('free processors:%d',$self->{free_processors});
    
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
    _kill_data_processors($self->{processors});
    #semctl($self->{sem_id},0,IPC_RMID,0);
}

sub semaphore_up { 
    my ($sem_id) = @_;
    $sem_id->up; return;
    my $sem_op = pack("s!*",
        0, 0, 0, # wait until zero
        0, 1, 0, # up
    );
    semop($sem_id, $sem_op)   || die "semop: $!";
}

sub semaphore_down {
    my ($sem_id) = @_;
    $sem_id->down; return;
    my $sem_op = pack("s!*",
        0, -1, 0, # down
    );
    semop($sem_id, $sem_op)   || die "semop: $!";
}


use Data::Dump qw(dump);
sub debug {
    return;
	my ($format,@par) = @_;
    return unless $format =~ m{sema};
	my ($package, $filename, $line) = caller;
	printf STDERR "%s(%d) $format\n",$filename,$line,map {defined($_)?(ref($_)?dump($_):$_):'undef'} @par;
}


1;
