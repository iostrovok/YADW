package YADW;
# https://metacpan.org/module/DBI#Subclassing-the-DBI

use DBI;
use SQL::Abstract::Limit;

use strict;
use warnings;

use vars qw(@ISA);
@ISA = qw(DBI);

use YADW::db;
use YADW::st;



1;
