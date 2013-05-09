use strict;
use warnings;

use Test::More tests => 15;
BEGIN { use_ok('Parallel::DataPipe') };

my @data;
@data = 1..1000; 
my @processed_data;
Parallel::DataPipe::run {
    input_data => [@data],
    process_data => sub { $_*2 },
    merge_data => sub { push @processed_data, $_; },
};

ok(@data==@processed_data,'length of processed scalar data');
ok(join(",",map $_*2, @data) eq join(",",sort {$a <=> $b} @processed_data),"processed scalar data values");
ok(zombies() == 0,'no zombies');

# test pipe for serialized data
@data = map [$_],1..1000; 
@processed_data = ();
Parallel::DataPipe::run {
    input_data => [@data],
    process_data => sub { [$_->[0]*2] },
    merge_data => sub { push @processed_data, $_; },
};

ok(@data==@processed_data,'length of processed serialized data');
ok(join(",",map $_->[0]*2, @data) eq join(",",sort {$a <=> $b} map $_->[0],@processed_data),"processed serialized data values");
ok(zombies() == 0,'no zombies');

#test large data
my $large_data_size = 1024*1024*64; # 64Mb
my $large_data_buf = sprintf("%${large_data_size}s"," ");
@data = map [$large_data_buf],1..4;
@processed_data = ();
Parallel::DataPipe::run {
    input_data => [@data],
    process_data => sub { $_ },
    merge_data => sub { push @processed_data, $_; },
};
my $big = sprintf("big(%dk)",$large_data_size/1024);
ok(@data==@processed_data,"length of $big data");
ok(grep($_->[0] eq $large_data_buf,@processed_data) == @data,"processed $big data values");
ok(zombies() == 0,'no zombies');


# test processor_number
my $processor_number = 128;
my $n;
my %forks; # calculates counter of items processed by each data processor, so keys will be pid of data processor
my @pd = (
    input_data => sub { $n--|| undef },
    process_data => sub {$$},
    processor_number => $processor_number,
    merge_data => sub {$forks{$_}++},
);

my $data_item_per_thread = 4;
$n=$processor_number*$data_item_per_thread;
Parallel::DataPipe::run {@pd};
ok(keys(%forks) == $processor_number,"explicit number of data_processors($processor_number)- although it depends on implementation");
ok(zombies() == 0,'no zombies');

# this test is not critical, so only warn
my %processor_load;
$processor_load{$_}++ for values %forks;
warn "processor load not aligned (has to be $data_item_per_thread:$processor_number):".join(",",map "$_:$processor_load{$_}",keys %processor_load) unless  keys(%processor_load)==1;

# test other chlidren survive and not block
my $child = fork();
if ($child == 0) {
    # in normal situation it will be killed by parent
    # unless it will block Parallel::DataPipe::run to return
    # but no more then for 10 seconds
    # it relies next Parallel::DataPipe::run can't be executed more then 20 seconds
    sleep(20);
    exit;
}
$n=$processor_number*10;
Parallel::DataPipe::run {@pd};
ok(kill(1,$child)==1,'other child alive');
ok(wait == $child,'killed & ripped by parent');
ok(zombies() == 0,'no zombies');

use POSIX ":sys_wait_h";
sub zombies {
    my $kid = waitpid(-1, WNOHANG);
    return $kid > 0;
}
