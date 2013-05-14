use 5.008;
use ExtUtils::MakeMaker;
use IO::Select;
# lets check if pipe works here as intented
print "check if fork & pipes work on a platform $^O...\n";
eval {
    pipe(my $pipe_read,my $pipe_write);    
    my $pid = fork();
    if ($pid == 0) {
        print $pipe_write 'PIPE';
        $pipe_write->flush();
        exit;
    }
    my ($handle) = IO::Select->new($pipe_read)->can_read(0.1);
    die "select returned no handle" unless $handle;
    read($handle,my $buf, 4);
    die "buffer was spoiled" unless $buf eq 'PIPE';
    1;
}  || die "Pipes does not work as intented on this platform, sorry($@)!";
print "pipes looks good!\n";
# See lib/ExtUtils/MakeMaker.pm for details of how to influence
# the contents of the Makefile that is written.
WriteMakefile(
    NAME              => 'Parallel::DataPipe',
    VERSION_FROM      => 'lib/Parallel/DataPipe.pm', # finds $VERSION
    PREREQ_PM         => {}, # e.g., Module::Name => 1.1
    ($] >= 5.005 ?     ## Add these new keywords supported since 5.005
      (ABSTRACT_FROM  => 'lib/Parallel/DataPipe.pm', # retrieve abstract from module
       AUTHOR         => 'Oleksandr Kharchenko <okharch@okharch.com>') : ()),
);
