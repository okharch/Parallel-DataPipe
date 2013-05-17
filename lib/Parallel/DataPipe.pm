package Parallel::DataPipe;

our $VERSION='0.04';
use 5.008; # Perl::MinimumVersion says that

use strict;
use warnings;

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

# conveyor is platform dependent, default is POSIX implementation
sub _create_data_process_conveyor {
    my $platform = $^O;
    my $class = __PACKAGE__ ."::$platform";
    unless (eval("require $class;1") && $class->can('new')) {
        $class = __PACKAGE__ ."::KISS";
        eval "require $class" unless $class->can('new');
    }
    return $class->new(@_);    
}

sub run {
    my $param = shift;
    my $input_iterator = _get_input_iterator(delete $param->{'input_iterator'});
    my $conveyor = _create_data_process_conveyor($param);
    
    #local $SIG{ALRM} = 'IGNORE'; # _wait_pipe overides this
    # data processing conveyor. 
    while (defined(my $data = $input_iterator->())) {
        $conveyor->process_data($data);
    }
    
    # receive and merge remaining data from busy processors
    my $busy_processors = $conveyor->busy_processors; # number of busy processors
    Parallel::DataPipe::KISS::debug('receive remaining:%d',$busy_processors);
    $conveyor->receive_and_merge_data() while $busy_processors--;
    Parallel::DataPipe::KISS::debug('run finished',$busy_processors);
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
