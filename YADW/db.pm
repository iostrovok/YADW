package YADW::db;

@YADW::db::ISA = qw(DBI::db);

use strict;
use Data::Dumper;

our $DEBUG = 0;
our $AUTOLOAD;

my %ACTIONS = map {( $_ => 1 )} qw[ all col cols count delete findorcreate findorinsert foc foi get getfun getrow insert ivalue procedure row uoi update updateorinsert value ];

sub connected {
    my ( $self, $dns, $user, $pass, $add ) = @_;

    $self->{private_yadw} = {
        RETURNING		=> {},
        RETURNING_LIST	=> [],
        is_return		=> 0,
        is_return_inner	=> 0,
        to_utf8			=> 0,
        debug			=> 0,
        is_die			=> 1,
        last_sub		=> '',
        limit_dialect	=> 'LimitOffset',
    };

    if ( UNIVERSAL::isa( $add, 'HASH' ) && UNIVERSAL::isa( $add->{private_yadw}, 'HASH' )) {
        $self->{private_yadw} = { %{$self->{private_yadw}}, %{$add->{private_yadw}} };
    }

    my $limit_dialect = $self->{private_yadw}{limit_dialect};

    $self->{private_yadw}{sql} = new SQL::Abstract::Limit( limit_dialect => $self->{private_yadw}{limit_dialect} );
}

# RETURNING setters & getters
sub set_returning	{ $_[0]->{private_yadw}{is_return} = 1; }
sub clean_returning { $_[0]->{private_yadw}{RETURNING} = {}; $_[0]->{private_yadw}{RETURNING_LIST} = {}; $_[0]->{private_yadw}{is_return} = 0; }
sub returning		{ $_[0]->{private_yadw}{RETURNING}; }
sub returning_list	{ $_[0]->{private_yadw}{RETURNING_LIST}; }

sub error_log {
	my $self = shift;
	print STDERR join("\n", @_), "\n";
}

sub alert_log {
	my $self = shift;
	return unless $self->{private_yadw}{debug};
	die "Please redifind this function in your code.";
}

sub doit {
	my $self = shift;
	my $slq_line = shift;
	$self->{private_yadw}{last_sub} = "doit";

	my @out = ();
	eval {
		my $sth = $self->SUPER::prepare($slq_line) or die DBI->errstr;
		$sth->execute( @_ ) or die DBI->errstr;
	};

	if ( $@ ) {
		$self->send_log( $@, $slq_line, Dumper(\@_) );
		die $@;
	}
}

sub simple {
	my $self = shift;
	my $slq_line = shift;
	$self->{private_yadw}{last_sub} = "simple";

	my @out = ();
	eval {
		my $sth = $self->SUPER::prepare($slq_line) or die DBI->errstr;
		$sth->execute( @_ ) or die DBI->errstr;
		while ( my $hash_ref = $sth->fetchrow_hashref ) {
			push ( @out, $hash_ref );
		}
	};

	if ( $@ ) {
		$self->send_log( $@, $slq_line, Dumper(\@_) );
		die $@;
	}
	return [@out];
}

sub getrow_simple {
	my $self = shift;
	my $slq_line = shift;
	$self->{private_yadw}{last_sub} = "getrow_simple";

	my $out;
	eval {
		my $sth = $self->SUPER::prepare($slq_line) or die DBI->errstr;
		$sth->execute( @_ ) or die DBI->errstr;
		$out = $sth->fetchrow_hashref;
	};

	if ( $@ ) {
		$self->send_log( $@, $slq_line, Dumper(\@_) );
		die $@;
	}
	return $out;
}

sub DESTROY {}
sub AUTOLOAD {
	my $self  = shift;
	my( $slq_line, @bind, @out, $sth, $err, $table );

	my $full_name = $AUTOLOAD;
	$AUTOLOAD =~ s/.*:://;
	my ( $act, $schema, $sm_table ) = $AUTOLOAD =~ m/^([^_]+)_([^_]+)_(.+)/gios;

	if ( exists $ACTIONS{$act} ) {

		$table =  $schema .'.'. $sm_table ;
		_include_sub ( $full_name, $act, $table );

		my $name = $self->{private_yadw}{last_sub} = "$AUTOLOAD";
		return $self->$name(@_);
    }

	$self->error_log('BAD ACTION:: "'. $act, '", BAD ACTION:: "'. join('#', @_), '"' );
	die 'BAD ACTION:: "'. $act, '", BAD ACTION:: "'. join('#', @_), '"';
}

sub yadw_clean {
	$_[0]->{private_yadw}{RETURNING} = {};
	$_[0]->{private_yadw}{RETURNING_LIST} = [];
	$_[0]->{private_yadw}{last_sub} = '';
}

sub _include_sub {
	my ( $full_name, $act, $table ) = @_;

	no strict 'refs';

	my $name = $full_name;
	$name =~ s/.*:://;

	local $DEBUG = 0;

	if ( $act eq 'all' ||  $act eq 'get' || $act eq 'cols' ) {
		return *$full_name = sub {
				my $self = shift; $self->yadw_clean(); $self->{private_yadw}{last_sub} = $name;
				my( $slq_line, @bind, @out, $sth, $err );
				my @params = @_;
				unshift @params, ['*'] unless $act eq 'cols';
				eval {
					( $slq_line, @bind ) = $self->{private_yadw}{sql}->select( $table, @params );
					$sth = $self->SUPER::prepare($slq_line) or die "SUB: $name, ". DBI->errstr;
					$err = $sth->execute( @bind ) or die "SUB: $name, ". DBI->errstr;
					while ( my $hash_ref = $sth->fetchrow_hashref ) {
						push ( @out, $hash_ref );
					}
				};

				return \@out;
			};
	}

	if ( $act eq 'col' ||  $act eq 'getfun' ) {
		return *$full_name = sub {
				my $self = shift; $self->yadw_clean(); $self->{private_yadw}{last_sub} = $name;
				my( $slq_line, @bind, @out, $sth, $err );
				eval {
					( $slq_line, @bind ) = $self->{private_yadw}{sql}->select( $table, @_ );
					$sth = $self->SUPER::prepare($slq_line) or die "SUB: $name, ". DBI->errstr;
					$err = $sth->execute( @bind ) or die "SUB: $name, ". DBI->errstr;

					while ( my @a = $sth->fetchrow_array ) {
						push ( @out, @a );
					}
				};

				return @out;
			};
	}

	if ( $act eq 'count' ) {
		*$full_name = sub {
				my $self = shift; $self->yadw_clean(); $self->{private_yadw}{last_sub} = $name;
				my( $slq_line, @bind, $count, $sth, $err );
				eval {
					( $slq_line, @bind ) = $self->{private_yadw}{sql}->select( $table, [' count(*) as counters '], @_ );
					$sth = $self->SUPER::prepare($slq_line) or die "SUB: $name, ". DBI->errstr;
					$err = $sth->execute( @bind ) or die "SUB: $name, ". DBI->errstr;
					my $hash_ref = $sth->fetchrow_hashref;

					unless ( $hash_ref && ref $hash_ref eq 'HASH' ) {
						$count = 0;
					} else {
						$count = int( $hash_ref->{counters} || 0  );
					}
				};
				return $count;
			};
	}

	if ( $act eq 'getrow' || $act eq 'row' ) {
		return *$full_name = sub {
				my $self = shift; $self->yadw_clean(); $self->{private_yadw}{last_sub} = $name;
				my( $slq_line, @bind, $hash_ref, $sth, $err );
				eval {
					( $slq_line, @bind ) = $self->{private_yadw}{sql}->select( $table, ['*'], @_, '', 0, 1);
					$sth = $self->SUPER::prepare($slq_line) or die "SUB: $name, ". DBI->errstr;
					$err = $sth->execute( @bind ) or die "SUB: $name, ". DBI->errstr;
					$hash_ref = $sth->fetchrow_hashref;
				};
				return {} unless UNIVERSAL::isa( $hash_ref, 'HASH' );
				return $hash_ref;
			};
	}

	if ( $act eq 'update' ) {
		return *$full_name = sub {
				my $self = shift; $self->yadw_clean(); $self->{private_yadw}{last_sub} = $name;
				my( $slq_line, @bind, @out, $sth, $err );
				eval {
					( $slq_line, @bind ) = $self->{private_yadw}{sql}->update( $table, @_ );
					$slq_line .= ' RETURNING * ' if $self->{private_yadw}{is_return};

					$sth = $self->SUPER::prepare($slq_line) or die "SUB: $name, ". DBI->errstr;
					$err = $sth->execute( @bind ) or die "SUB: $name, ". DBI->errstr;

					if ( $self->{private_yadw}{is_return} ) {
						while ( my $hash_ref = $sth->fetchrow_hashref ) {
							push ( @out, $hash_ref );
						}
						$self->{private_yadw}{RETURNING} = $out[0] || {};
						$self->{private_yadw}{RETURNING_LIST} = [ @out ] ;
					}
				};
				return $err;
			};
	}

	if ( $act eq 'insert' ) {
		return *$full_name = sub {
				my $self = shift; $self->yadw_clean(); $self->{private_yadw}{last_sub} = $name;
				my( $slq_line, @bind, @out, $sth, $err );
				eval {
					( $slq_line, @bind ) = $self->{private_yadw}{sql}->insert( $table, @_ );
					$slq_line .= ' RETURNING * ' if $self->{private_yadw}{is_return};

					$sth = $self->SUPER::prepare($slq_line) or die "SUB: $name, ". DBI->errstr;
					$err = $sth->execute( @bind ) or die "SUB: $name, ". DBI->errstr;

					if ( $self->{private_yadw}{is_return} ) {
						while ( my $hash_ref = $sth->fetchrow_hashref ) {
							push ( @out, $hash_ref );
						}
						$self->{private_yadw}{RETURNING} = $out[0] || {};
						$self->{private_yadw}{RETURNING_LIST} = [ @out ] ;
					}
				};
				return $err;
			};
	}

	if ( $act eq 'delete' ) {
		return *$full_name = sub {
				my $self = shift; $self->yadw_clean(); $self->{private_yadw}{last_sub} = $name;
				my( $slq_line, @bind, @out, $sth, $err );
				eval {
					( $slq_line, @bind ) = $self->{private_yadw}{sql}->delete( $table, @_ );
					$sth = $self->SUPER::prepare($slq_line) or die "SUB: $name, ". DBI->errstr;
					$err = $sth->execute( @bind ) or die "SUB: $name, ". DBI->errstr;
				};
				return $err;
			};
	}

	if ( $act eq 'procedure' ) {
		return *$full_name = sub {
				my $self = shift; $self->yadw_clean(); $self->{private_yadw}{last_sub} = $name;
				my( $slq_line, @bind, @out, $sth, $err );
				eval {

					@bind = @{$_[0]} if UNIVERSAL::isa( $_[0], 'ARRAY' );
					$slq_line = "select $table(". join(',', ( split('', '?' x scalar( @bind ) ) ) ) .") AS res ";
					$sth = $self->SUPER::prepare($slq_line) or die "SUB: $name, ". DBI->errstr;
					$err = $sth->execute( @bind ) or die "SUB: $name, ". DBI->errstr;
				};
				return $err;
			};
	}

	if ( $act eq 'ivalue' || $act eq 'value' ) {
		return *$full_name = sub {
				my $self = shift; $self->yadw_clean(); $self->{private_yadw}{last_sub} = $name;
				my( $slq_line, @bind, $hash_ref, $sth, $err );
				eval {
					( $slq_line, @bind ) = $self->{private_yadw}{sql}->select( $table, @_ );
					$sth = $self->SUPER::prepare($slq_line) or die "SUB: $name, ". DBI->errstr;
					$err = $sth->execute( @bind ) or die "SUB: $name, ". DBI->errstr;
					$hash_ref = $sth->fetchrow_hashref;
				};

				if ( $act eq 'ivalue' ) {
					return 0 unless UNIVERSAL::isa( $hash_ref, 'HASH' );
					return int ( (values %$hash_ref)[0] || 0 );
				}

				if ( $act eq 'value' ) {
					return undef unless UNIVERSAL::isa( $hash_ref, 'HASH' );
					return (values %$hash_ref)[0];
				}
			};
	}

	if ( $act eq 'findorcreate' or $act eq 'findorinsert' or $act eq 'foi' or $act eq 'foc' ) {
		return *$full_name = sub {
				my $self = shift; $self->yadw_clean(); $self->{private_yadw}{last_sub} = $name;
				my( $slq_line, @bind, $hash_ref, $sth, $err, @out );
				eval {
					( $slq_line, @bind ) = $self->{private_yadw}{sql}->select( $table, ['*'], $_[1], '', 0, 1);
					$sth = $self->SUPER::prepare($slq_line) or die "SUB: $name, ". DBI->errstr;
					$err = $sth->execute( @bind ) or die "SUB: $name, ". DBI->errstr;
					$hash_ref = $sth->fetchrow_hashref;
				};
				return $hash_ref if UNIVERSAL::isa( $hash_ref, 'HASH' ) && %$hash_ref;

				eval {
					( $slq_line, @bind ) = $self->{private_yadw}{sql}->insert( $table, { %{$_[1]}, %{$_[0]} } );
					$slq_line .= ' RETURNING * ';

					$sth = $self->SUPER::prepare($slq_line) or die "SUB: $name, ". DBI->errstr;
					$err = $sth->execute( @bind ) or die "SUB: $name, ". DBI->errstr;

					my $hash_ref = $sth->fetchrow_hashref;
					$self->{private_yadw}{RETURNING} = $hash_ref && %$hash_ref ? $hash_ref : {};
					$self->{private_yadw}{RETURNING_LIST} = [ $self->{private_yadw}{RETURNING} ];
				};
				return $self->{private_yadw}{RETURNING};
			};
	}

	if ( $act eq 'updateorinsert' or $act eq 'uoi' ) {
		return *$full_name = sub {
				my $self = shift; $self->yadw_clean(); $self->{private_yadw}{last_sub} = $name;
				my( $slq_line, @bind, $hash_ref, $sth, $err, @out );

				eval {
					( $slq_line, @bind ) = $self->{private_yadw}{sql}->select( $table, [' count(*) as counters '], $_[1] );
					$sth = $self->SUPER::prepare($slq_line) or die "SUB: $name, ". DBI->errstr;
					$err = $sth->execute( @bind ) or die "SUB: $name, ". DBI->errstr;
					my $hash_ref = $sth->fetchrow_hashref;

					if ( UNIVERSAL::isa( $hash_ref, 'HASH' ) && int( $hash_ref->{counters} || 0 ) > 0 ) {
						( $slq_line, @bind ) = $self->{private_yadw}{sql}->update( $table, @_ );
						$slq_line .= ' RETURNING * ' if $self->{private_yadw}{is_return};

						$sth = $self->SUPER::prepare($slq_line) or die "SUB: $name, ". DBI->errstr;
						$err = $sth->execute( @bind ) or die "SUB: $name, ". DBI->errstr;

						if ( $self->{private_yadw}{is_return} ) {
							while ( my $hash_ref = $sth->fetchrow_hashref ) {
								push ( @out, $hash_ref );
							}
							$self->{private_yadw}{RETURNING} = $out[0] || {};
							$self->{private_yadw}{RETURNING_LIST} = [ @out ] ;
						}
					} else {
						( $slq_line, @bind ) = $self->{private_yadw}{sql}->insert( $table, { %{$_[1]}, %{$_[0]} }  );
						$slq_line .= ' RETURNING * ' if $self->{private_yadw}{is_return};

						$sth = $self->SUPER::prepare($slq_line) or die "SUB: $name, ". DBI->errstr;
						$err = $sth->execute( @bind ) or die "SUB: $name, ". DBI->errstr;

						if ( $self->{private_yadw}{is_return} ) {
							while ( my $hash_ref = $sth->fetchrow_hashref ) {
								push ( @out, $hash_ref );
							}
							$self->{private_yadw}{RETURNING} = $out[0] || {};
							$self->{private_yadw}{RETURNING_LIST} = [ @out ] ;
						}
					}
				};

				return $err;
			};
	}

}

1;
