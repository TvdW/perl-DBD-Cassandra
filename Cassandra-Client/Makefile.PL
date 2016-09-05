use 5.008;
use strict;
use warnings;
use ExtUtils::MakeMaker;

WriteMakefile(
    NAME             => 'Cassandra::Client',
    AUTHOR           => q{Tom van der Woerdt <tvdw@cpan.org>},
    VERSION_FROM     => 'lib/Cassandra/Client.pm',
    ABSTRACT_FROM    => 'lib/Cassandra/Client.pm',
    LICENSE          => 'perl_5',
    PL_FILES         => {},
    MIN_PERL_VERSION => 5.008,
    CONFIGURE_REQUIRES => {
        'ExtUtils::MakeMaker' => 0,
    },
    BUILD_REQUIRES => {
        'Test::More' => 0,
    },
    PREREQ_PM => {
        #'ABC'              => 1.6,
        #'Foo::Bar::Module' => 5.0401,
        'Ref::Util' => '0.008',
        'Clone' => '0.38',
        # Optional: AnyEvent, Promises, Compress::LZ4, Compress::Snappy
    },
    dist  => { COMPRESS => 'gzip -9f', SUFFIX => 'gz', },
    clean => { FILES => 'Cassandra-Client-*' },
);
