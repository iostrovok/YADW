use 5.006;
use strict;
use warnings FATAL => 'all';
use Module::Build;

my $builder = Module::Build->new(
    module_name         => 'YADW',
    license             => 'Artistic_2_0',
    dist_author         => q{Lashko VLadimir <ostrovok@gmail.com>},
    dist_version_from   => 'lib/YADW.pm',
    release_status      => 'stable',
    configure_requires => {
        'Module::Build' => 0,
    },
    build_requires => {
        'Test::More' => 0,
    },
    requires => {
        'SQL::Abstract::Limit ' => 0.141,
        'DBI' => 1.628,
    },
    add_to_cleanup     => [ 'YADW-*' ],
    create_makefile_pl => 'traditional',
);

$builder->create_build_script();
