/*
 *   This file is part of TISEAN
 *
 *   Copyright (c) 1998-2007 Rainer Hegger, Holger Kantz, Thomas Schreiber
 *
 *   TISEAN is free software; you can redistribute it and/or modify
 *   it under the terms of the GNU General Public License as published by
 *   the Free Software Foundation; either version 2 of the License, or
 *   (at your option) any later version.
 *
 *   TISEAN is distributed in the hope that it will be useful,
 *   but WITHOUT ANY WARRANTY; without even the implied warranty of
 *   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *   GNU General Public License for more details.
 *
 *   You should have received a copy of the GNU General Public License
 *   along with TISEAN; if not, write to the Free Software
 *   Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA  02110-1301  USA
 */
/*Author: Rainer Hegger Last modified: Sep 5, 2004*/
/* Changes: 
 * Sep 5 2004: + include <stdlib.h>
 */

#include <stdio.h>
#include "tisean_cec.h"
#include <stdlib.h>

void rescale_data(double *x,unsigned long l,double *min,double *interval)
{
  int i;
  
  *min=*interval=x[0];
  
  for (i=1;i<l;i++) {
    if (x[i] < *min) *min=x[i];
    if (x[i] > *interval) *interval=x[i];
  }
  *interval -= *min;

  if (*interval != 0.0) {
    for (i=0;i<l;i++)
      x[i]=(x[i]- *min)/ *interval;
  }
  else {
    char *m  = PyOS_double_to_string(*min,             'r', 0, Py_DTSF_ADD_DOT_0, NULL);
    char *mi = PyOS_double_to_string(*min + *interval, 'r', 0, Py_DTSF_ADD_DOT_0, NULL);
    PyErr_Format(PyExc_RuntimeError, "rescale_data: data ranges from %s to %s. It makes no sense to continue.",m,mi);
  }
}
