use strict;
use warnings;

use Test::More tests => 21;
use Time::HiRes qw(time);
BEGIN { use_ok('Parallel::DataPipe') };

#printf "You may top -p%s\n",$$;sleep(2);

# constant for max processor number tests: test_processor_number & test_other_children_survive
my $number_of_data_processors = 32;
my $n_items = 4; # number of large item to process
my $mb = 1024*1024;

test_scalar_values();
test_serialized_data();
test_processor_number();
test_other_children_survive();

# before testing big data let's top to see memory occupied
#printf "Have a chance loot top -p%s ...\n",$$;<>;

test_large_data_send(); # this test is fork ability killer, memory expander, can't make a (lot of) fork after it

#printf "Have a chance loot top -p%s ...\n",$$;<>;
test_large_data_receive(); # this test is fork ability killer, memory expander, can't make a (lot of) fork after it

test_large_data_process(); # this test is fork ability killer, memory expander, can't make a (lot of) fork after it

print "\n***Done!\n";

exit 0;

sub test_scalar_values {
    print "\n***Testing if conveyor works ok with simple scalar data...\n";
    my @data = 1..1000; 
    my @processed_data = ();
    Parallel::DataPipe::run {
        input_iterator => \@data,
        process_data => sub { $_*2 },
        merge_data => sub { push @processed_data, $_; },
    };
    
    ok(@data==@processed_data,'length of processed scalar data');
    ok(join(",",map $_*2, @data) eq join(",",sort {$a <=> $b} @processed_data),"processed scalar data values");
    #printf "processed data:%s\n",join ",",@processed_data;
    ok(zombies() == 0,'no zombies');
}

sub test_serialized_data {
    print "\n***Testing if conveyor works ok with serizalized data...\n";
    # test pipe for serialized data
    my @data = map [$_],1..1000; 
    my @processed_data = ();
    Parallel::DataPipe::run {
        input_iterator => \@data,
        process_data => sub {
            [$_->[0]*2];
        },
        merge_data => sub { push @processed_data, $_; },
    };
    
    ok(@data==@processed_data,'length of processed serialized data');
    ok(join(",",map $_->[0]*2, @data) eq join(",",sort {$a <=> $b} map $_->[0],@processed_data),"processed serialized data values");
    ok(zombies() == 0,'no zombies');
}

sub test_large_data_receive {
    #test large data
    my $large_data_size = max_buf_size(32); # 64Mb
    my $big = sprintf("big(%dM)",$large_data_size/$mb);
    print "\n***Testing if data processor receives ok $big buffer wrapped into [$n_items] array...\n";
    my $large_data_buf = sprintf("%${large_data_size}s"," ");
    my @data = map [$large_data_buf],1..$n_items;
    $large_data_buf =~ s/ {8}/!!!!!!!!/;
    my @processed_data = ();
    my $time = time();
    Parallel::DataPipe::run {
        input_iterator => \@data,
        process_data => sub { $_->[0] =~ s/ {8}/!!!!!!!!/;my $ret = $_->[0] eq $large_data_buf?1:0;undef $_; $ret },
        number_of_data_processors => 4,
        merge_data => sub { push @processed_data, $_; },
    };
    my $elapsed = time - $time;
    #print substr($large_data_buf,0,20)."#\n";
    #print substr($processed_data[0][0],0,20)."#\n";
    ok(@data==@processed_data,"length of received $big data");
    ok(grep($_ == 1,@processed_data) == @data,"received $big data values");
    ok(zombies() == 0,'no zombies');
    # try to clean memory
    undef $_;
    $_ = undef for $large_data_buf,map $_->[0],@data;
    my $bytes = $large_data_size * $n_items;
    printf "%d Mb have been received from parent & processed %.3f seconds, throughput %.1f Mb/sec\n",$bytes/$mb,$elapsed,$bytes/($mb*$elapsed);
};


sub test_large_data_send {
    #test large data
    my $large_data_size = max_buf_size(16);
    my $big = sprintf("big(%dM)",$large_data_size/$mb);
    print "\n***Testing if data processor sends ok $big buffer wrapped into array...\n";
    my $large_data_buf = sprintf("%${large_data_size}s"," ");
    $large_data_buf =~ s/ {8}/!!!!!!!!/;
    my @processed_data = ();
    my $n = $n_items;
    my $time = time();
    Parallel::DataPipe::run {
        input_iterator => sub {$n-- > 0||undef },
        process_data => sub { [$large_data_buf] },
        number_of_data_processors => 3,
        merge_data => sub { push @processed_data, $_->[0] eq $large_data_buf?1:0 },
    };
    my $elapsed = time - $time;
    #print substr($large_data_buf,0,20)."#\n";
    #print substr($processed_data[0][0],0,20)."#\n";
    ok($n_items==@processed_data,"length of sent $big data");
    ok($n_items==grep($_ == 1,@processed_data),"sent $big data values");
    ok(zombies() == 0,'no zombies');
    # try to clean memory
    undef $_;
    $_ = undef for $large_data_buf;
    my $bytes = $large_data_size * $n_items;
    printf "%d Mb have been sent from processor to parent in a %.3f seconds, throughput %.1f Mb/sec\n",$bytes/$mb,$elapsed,$bytes/(1024*1024*$elapsed);
};

sub test_large_data_process {
    #test large data
    my $large_data_size = max_buf_size(32); # 64Mb
    my $big = sprintf("big(%dM)",$large_data_size/$mb);
    print "\n***Testing conveyor (send,process,receive) of $big buffer wrapped into array...\n";
    my $large_data_buf = sprintf("%${large_data_size}s"," ");
    $large_data_buf =~ s/ {8}/!!!!!!!!/;
    my @processed_data = ();
    my $time = time();
    my $n = $n_items;
    Parallel::DataPipe::run {
        input_iterator => sub {$n-- > 0?[$large_data_buf]:undef},
        process_data => sub { $_->[0] =~ s/!{8}/        /;[$_->[0]] },
        number_of_data_processors => 4,
        merge_data => sub { push @processed_data, $_->[0] =~ m{^ {8} }?1:0 },
    };
    my $elapsed = time - $time;
    #print substr($large_data_buf,0,20)."#\n";
    #print substr($processed_data[0][0],0,20)."#\n";
    ok($n_items==@processed_data,"length of received $big data");
    ok($n_items==grep($_ == 1,@processed_data),"received $big data values");
    ok(zombies() == 0,'no zombies');
    # try to clean memory
    undef $_;
    $_ = undef for $large_data_buf;
    my $bytes = $large_data_size * $n_items * 2;
    printf "%d Mb have been sent,processed & received in %.3f seconds, throughput %.1f Mb/sec\n",$bytes/$mb,$elapsed,$bytes/($mb*$elapsed);
};


sub test_processor_number {
    print "\n***Testing if conveyor works ok big($number_of_data_processors) number of data processors...\n";
    
    # test number_of_data_processors
    my $n;
    my %forks; # calculates counter of items processed by each data processor, so keys will be pid of data processor
    
    my $data_item_per_thread = 4;
    $n=$number_of_data_processors*$data_item_per_thread;
    Parallel::DataPipe::run {
        input_iterator => sub { $n-- > 0|| undef },
        process_data => sub {$$},
        number_of_data_processors => $number_of_data_processors,
        merge_data => sub {$forks{$_}++},
    };
    ok(keys(%forks) == $number_of_data_processors,"explicit number of data_processors($number_of_data_processors)- although it depends on implementation");
    ok(zombies() == 0,'no zombies');
    
    return;
    # this test is not critical, so only warn
    my %processor_load;
    $processor_load{$_}++ for values %forks;
    warn "processor load not aligned (has to be $data_item_per_thread:$number_of_data_processors):".join(",",map "$_:$processor_load{$_}",keys %processor_load) unless  keys(%processor_load)==1;
}

sub test_other_children_survive {
    print "\n***Fork children, then run DataPipe conveyor and looks if our children survives after it...\n";
    
    # test other chlidren survive and not block
    my $child = fork();
    if ($child == 0) {
        # in normal situation it will be killed by parent
        # unless it will block Parallel::DataPipe::run to return
        # but no more then for 10 seconds
        # it relies next Parallel::DataPipe::run can't be executed more then 20 seconds
        sleep(2);
        exit;
    }
    my $n=$number_of_data_processors*10;
    Parallel::DataPipe::run {
        input_iterator => sub { $n-- > 0|| undef },
        process_data => sub {$$},
        number_of_data_processors => $number_of_data_processors,
        merge_data => sub {},
    };
    ok(kill(1,$child)==1,'other child alive');
    ok(wait == $child,'killed & ripped by parent');
    ok(zombies() == 0,'no zombies');
}

use POSIX ":sys_wait_h";
sub zombies {
    #printf "looking for sombies...\n";
    my $kid = waitpid(-1, WNOHANG);
    return $kid > 0;
}


sub max_buf_size { my $d = shift;
    my ($memtotal,$memused);
    eval {
        ($memtotal,$memused) = map m{Mem:\s+(\d+)\s+(\d+)}, `free -b 2>/dev/null`;
    };
    my $free = defined($memtotal)?$memtotal-$memused:32*$mb;
    #print '$memtotal,$memused,$free:'."$memtotal,$memused,$free\n";
    my $r = $free / $d;
    # put reasonable limit for max buf size
    my $max_buf_size = 8 * $mb;
    $r = $max_buf_size if $r > $max_buf_size;
    return $r;
}
