.. _expression-syntax:

Maths Expression Syntax
=======================

The following describes the functions and operators supported by mathematical expressions within ``pprofit``.

The expression parser supports standard arithmetic operators and mathematical functions. Expressions are parsed using the `cexprtk <http://bitbucket.org/mjdr/cexprtk/>`_ python wrapper around the exprtk C++ library, although summarised below, more information about accepted syntax and available functions can be found here: http://partow.net/programming/exprtk/

	* Mathematical operators: ``+, -, *, /, %, ^``
	* Functions: ``min, max, avg, sum, abs, ceil, floor, round, roundn, exp, log, log10, logn, root, sqrt, clamp, inrange``
	* Trigonometry ``sin, cos, tan, acos, asin, atan, atan2, cosh, cot, csc, sec, sinh, tanh, d2r, r2d, d2g, g2d, hyp``
	* Equalities, Inequalities: ``==, <>, !=, <, <=, >, >=``
	* Boolean logic ``and, mand, mor, nand, nor, not, or, xor, xnor``
