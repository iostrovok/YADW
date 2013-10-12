#!/usr/bin/perl

use strict;
use warnings;
use feature qw[ say switch ];

use Data::Dumper;

use YADW;

# Transaction pooling
my %db = (
		host        => 'localhost',
        port        => '1332',

		user		=> 'sports',
        passwd		=> '',

		name		=> 'sports_monthly',

		auto_commit	=> 0,
		stable		=> 0,
);


say "Test 1";

#  dbi:DriverName:database=database_name;host=hostname;port=port

my $dns = "dbi:Pg:dbname=$db{name};host=$db{host};port=$db{port}";
say $dns;

my $dbh = YADW->connect( $dns, $db{user}, $db{passwd}, { AutoCommit => 0, AddParams => 2  });


say Dumper($dbh);


#$dbh->prepare("select * from articles limit 10");

my $get = $dbh->get_public_articles({}, 10, 10 );

say Dumper($get);