( User-defined functions in Forth syntax )
( Syntax: : name body ; )

( Select all-null columns in Meta view )
: sel_null sel_rows `null%` == '100.0' ;

( Select single-value columns in Meta view )
: sel_single sel_rows distinct == '1' ;
