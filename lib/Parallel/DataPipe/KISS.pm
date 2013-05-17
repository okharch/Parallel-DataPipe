# this basic (my favorite keep it simple stupid) implementation
# should work ok for all platforms with fork support
package Parallel::DataPipe::KISS;

use 5.008; # Perl::MinimumVersion says that

use strict;
use warnings;
require Storable;
use File::Slurp qw(read_file write_file);
use File::Temp qw(tempdir);
use Carp qw(confess);

my $parent = $$;

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

sub store {
	my $self = shift;
	$self->{store}->(@_);
}

sub retrieve {
	my $self = shift;
    confess('no file:'.$_[0]) unless -s $_[0];
	$self->{retrieve}->(@_);
}

# this inits store and retrieve with Storable subroutines
sub _init_serializer {
    my ($self,$param) = @_;
    my ($store, $retrieve) = grep $_ && ref($_) eq 'CODE',map delete $param->{$_},qw(store retrieve);
    if ($store && $retrieve) {
        $self->{store} = $store;
        $self->{retrieve} = $retrieve;
    } else {
        $self->{store} = \&Storable::nstore;
        $self->{retrieve} = \&Storable::retrieve;
    }
}


# this subroutine reads data from pipe and converts it to perl reference
sub _get_data {
    my ($self,$fh) = @_;
    my $data;
    if ($fh =~ m{\.dat$}) {
        $data = read_file $fh, binmode => ':raw';
    } else {
        $data = $self->retrieve($fh);
    }
    unlink ($fh);
    return $data;
}

# this store data to file. serialize if ref
sub _put_data {
    my ($self,$fh,$data) = @_;
    if (ref($data)) {
        $fh .= ".ref";
        $self->store($data,$fh);
    } else {
        $fh .= ".dat";
        write_file $fh, {binmode => ':raw'}, $data;
    }
}

sub _fork_data_processor {
    my ($data_processor_callback) = @_;
    # create processor as fork
    my $pid = fork();
    unless (defined $pid) {
        #print "say goodbye - can't fork!\n"; <>;
        die "can't fork!";
    }
    if ($pid == 0) {
        local $SIG{TERM} = sub {exit;}; # exit silently from data processors
        # data processor is eternal loop which wait for raw data on pipe from main
        # data processor is killed when it's not needed anymore by _kill_data_processors
        $data_processor_callback->() while (1);
        exit;
    }
    return $pid;
}

sub _wait_pipe {
    my ($self,$pipe,@alarm) = @_;
    my @files;
    debug("%s wait for %s",$$,$pipe);
    my $get_pipe = -d $pipe? sub {glob "$pipe/*"} : sub {grep -s, map "$pipe.$_", qw(dat ref) };
    while ( (@files = $get_pipe->()) == 0 ) {
        #_wakeup(@alarm) if @alarm;
        debug("%s wait (loop) for %s",$$,$pipe) unless $parent == $$;        
        eval {
            local $SIG{ALRM} = sub { die "Alarm!\n" };
            sleep;
        };       
    }
    debug("%s caught files %s",$$,\@files);
    wantarray? @files : $files[0];
}

sub _wakeup {
    kill 'SIGALRM', @_;
    #debug('wakeup %d',@_);
}

sub _create_data_processor {
    my ($self,$process_data_callback,$process_num) = @_;
    
    # row data pipe main => processor
    my $raw_data_pipe = sprintf("%s/%04d",$self->{raw_data_dir},$process_num);
    my $processed_data_pipe = sprintf("%s/processed-%04d",$self->{processed_data_dir},$process_num);
    
    my $parent = $$;
 
    my $data_processor = sub {
        my $pipe = $self->_wait_pipe($raw_data_pipe);#,$self->{data_processor_sleep}
        local $_ = $self->_get_data($pipe);
        # process data with given subroutine
        $_ = $process_data_callback->($_);
        # puts processed data back on pipe to main
        $self->_put_data($processed_data_pipe,$_);
        #_wakeup($parent);
    };
    
    # return data processor record 
    return {
        pid => _fork_data_processor($data_processor),  # needed to kill processor when there is no more data to process
        raw_data_pipe => $raw_data_pipe,                 # pipe to write raw data from main to data processor
        #processed_data_pipe => $processed_data_pipe,
    };
}

sub _create_data_processors {
    my ($self,$process_data_callback,$number_of_data_processors) = @_;
    
    $number_of_data_processors = $self->number_of_cpu_cores unless $number_of_data_processors;
    
    die "process_data parameter should be code ref" unless ref($process_data_callback) eq 'CODE';
	confess "\$number_of_data_processors:undefined" unless defined($number_of_data_processors);
    
    return [map $self->_create_data_processor($process_data_callback,$_), 1..$number_of_data_processors];
}

sub process_data {
	my ($self,$data) = @_;

    debug('process data:%s',$data);

    my $free_processor = $self->{free_processors}?
        $self->{free_processors}--
        :
        $self->receive_and_merge_data
    ;

    my $processor = $self->{processors}[$free_processor-1]; # free_processor is 1..n
    $processor->{item_number} = $self->{item_number}++;

    # put raw data to processor's pipe    
    $self->_put_data($processor->{raw_data_pipe},$data);
    
    debug('put item %d to processor %d (%s)',$processor->{item_number},$free_processor,$processor->{raw_data_pipe});

    # let processor know it can fetch the data from pipe
    kill('SIGALRM',$processor->{pid});
}

# this method is called once when input data EOF.
# to find out how many processors were involved in data processing
# then it calls receive_and_merge_data 1..busy_processors times
sub busy_processors {
    my $self = shift;
    my $result = @{$self->{processors}} - $self->{free_processors};
    debug('processors:%d busy:free_processors:%d, busy: %d',scalar(@{$self->{processors}}),$self->{free_processors},$result);
    return $result;
}

sub receive_and_merge_data {
	my $self = shift;
    # look if we have queue of processed items to merge
    my @processed = @{$self->{processed}};
    
    # wait for processed data unless we already have this queue
    my @alarm = map $_->{pid}, @{$self->{processors}};
    @processed = $self->_wait_pipe($self->{processed_data_dir},@alarm) unless @processed;
    
    # take first item from queue
    my $processed = shift(@processed);
    
    # and store the queue for subsequent calls
    $self->{processed} = \@processed;
    
    # read data from processed pipe
    local $_ = $self->_get_data($processed);
    
    # find out how to process it
    my ($processors,$data_merge_code) = @{$self}{qw(processors data_merge_code)};
    
    # find out process_num encoded to filename with data item
    my ($process_num) = $processed =~ m{processed-(\d+)\.};
    
    # this store the item number in source data pipe
    # it's cheap to support and may be someone need to reorder output item in source order
    my $item_number = $processors->[$process_num-1]{item_number};
    
    # merge processed data
    $data_merge_code->($_,$item_number);
    
    # returns the number of processor which is free now to process data
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
    # children sleep deply until are waked up by ALRM to read raw pipe
    $self->{data_processor_sleep} = delete $param->{data_processor_sleep} || 999;
    
    # this one sleep less: signal from child destroy the parent
    $self->{main_sleep} = delete $param->{main_sleep} || 999; 
    
    # item_number is number of item from source pipe
    # we support this as a second parameter for data_merge_code subroutine
    # in a case when it wants to reorder output items in source order
    $self->{item_number} = 0;
    
    # queue of procesed but not merged items
    $self->{processed} = [];
    
    # data_merge is sub which merge all processed data inside parent thread
    # it is called each time after process_data returns some new portion of data
    # it has data item both as $_ and $_[0] and also it has item_number as a $_[1]
    $self->{data_merge_code} = delete $param->{'merge_data'};
    die "data_merge should be code ref" unless ref($self->{data_merge_code}) eq 'CODE';
    
    # create bot raw_data_dir & processed_data_dir as a temporary, will remove it in destructor
    $self->{raw_data_dir} = tempdir( CLEANUP => 1 );
    $self->{processed_data_dir} = tempdir( CLEANUP => 1 );
    
    # check if user want to use alternative serialisation routines
    $self->_init_serializer($param);    

    # @$processors is array with data processor info
    $self->{processors} = $self->_create_data_processors(
        map delete $param->{$_},qw(process_data number_of_data_processors)
    );
    
    # this counts processors which are still not involved in processing data
    $self->{free_processors} = @{$self->{processors}};
    debug('free processors:%d',$self->{free_processors});
    
    my $not_supported = join ", ", keys %$param;
    die "Parameters are not supported:". $not_supported if $not_supported;
	
	return $self;
}

sub DESTROY {
	my $self = shift;
    _kill_data_processors($self->{processors});
}

use Data::Dump qw(dump);
sub debug {
	my ($format,@par) = @_;
	my ($package, $filename, $line) = caller;
    if (1) {
        printf STDERR "[%s]%s(%d) $format\n",$$,$filename,$line,map {defined($_)?(ref($_)?dump($_):$_):'undef'} @par if $$==$parent;
    } else {
        open my $fh, ">>","/tmp/$$";
        printf $fh "[%s]%s(%d) $format\n",$$,$filename,$line,map {defined($_)?(ref($_)?dump($_):$_):'undef'} @par if $$==$parent;
        close $fh;
    }
}


1;
