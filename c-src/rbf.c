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
/*Author: Rainer Hegger. Last modified: Mar 11, 2002 */
/*Modified by Daniel Cordeiro to include the Python wrapper. Last modified: May 6, 2014 */
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <limits.h>
#include "routines/tsa.h"
#include <math.h>
#include <Python.h>

#define WID_STR "Fits a RBF-model to the data"

#ifndef Py_PYTHON_H
  #error Python headers needed to compile C extensions, please install development version of Python.
#elif PY_VERSION_HEX < 0x03040000
  #error Requires Python 3.4+.
#else
#endif

char *outfile=NULL,stdo=1,MAKECAST=0;
char *infile=NULL;
char setdrift=1;
int DIM=2,DELAY=1,CENTER=2,STEP=1;
unsigned int COLUMN=1;
unsigned int verbosity=0xff;
long CLENGTH=1000;
unsigned long LENGTH=ULONG_MAX,INSAMPLE=ULONG_MAX,exclude=0;

double *series,*coefs;
double varianz,interval,min;
double **center;

#define pycheck_alloc(pnt)      \
  do {                          \
    if ((pnt) == NULL)          \
      return PyErr_NoMemory();  \
  } while(0)

double avdistance(void)
{
  int i,j,k;
  double dist=0.0;

  for (i=0;i<CENTER;i++)
    for (j=0;j<CENTER;j++)
      if (i != j)
        for (k=0;k<DIM;k++)
          dist += sqr(center[i][k]-center[j][k]);

  return sqrt(dist/(CENTER-1)/CENTER/DIM);
}

double rbf(double *act,double *cen)
{
  static double denum;
  double r=0;
  int i;

  denum=2.0*varianz*varianz;

  for (i=0;i<DIM;i++)
    r += sqr(*(act-i*DELAY)-cen[i]);

  return exp(-r/denum);
}

void drift(void)
{
  double *force,h,h1,step=1e-2,step1;
  int i,j,k,l,d2=DIM;

  pycheck_alloc(force=(double*)PyMem_RawMalloc(sizeof(double)*d2));
  for (l=0;l<20;l++) {
    for (i=0;i<CENTER;i++) {
      for (j=0;j<d2;j++) {
        force[j]=0.0;
        for (k=0;k<CENTER;k++) {
          if (k != i) {
            h=center[i][j]-center[k][j];
            force[j] += h/sqr(h)/fabs(h);
          }
        }
      }
      h=0.0;
      for (j=0;j<d2;j++)
        h += sqr(force[j]);
      step1=step/sqrt(h);
      for (j=0;j<d2;j++) {
        h1 = step1*force[j];
        if (((center[i][j]+h1) > -0.1) && ((center[i][j]+h1) < 1.1))
          center[i][j] += h1;
      }
    }
  }
  PyMem_RawFree(force);
}

void make_fit(void)
{
  double **mat,*hcen;
  double h;
  int i,j,n,nst;

  pycheck_alloc(mat=(double**)PyMem_RawMalloc(sizeof(double*)*(CENTER+1)));
  for (i=0;i<=CENTER;i++)
    pycheck_alloc(mat[i]=(double*)PyMem_RawMalloc(sizeof(double)*(CENTER+1)));
  pycheck_alloc(hcen=(double*)PyMem_RawMalloc(sizeof(double)*CENTER));

  for (i=0;i<=CENTER;i++) {
    coefs[i]=0.0;
    for (j=0;j<=CENTER;j++)
      mat[i][j]=0.0;
  }

  for (n=(DIM-1)*DELAY;n<INSAMPLE-STEP;n++) {
    nst=n+STEP;
    for (i=0;i<CENTER;i++)
      hcen[i]=rbf(&series[n],center[i]);
    coefs[0] += series[nst];
    mat[0][0] += 1.0;
    for (i=1;i<=CENTER;i++)
      mat[i][0] += hcen[i-1];
    for (i=1;i<=CENTER;i++) {
      coefs[i] += series[nst]*(h=hcen[i-1]);
      for (j=1;j<=i;j++)
        mat[i][j] += h*hcen[j-1];
    }
  }

  h=(double)(INSAMPLE-STEP-(DIM-1)*DELAY);
  for (i=0;i<=CENTER;i++) {
    coefs[i] /= h;
    for (j=0;j<=i;j++) {
      mat[i][j] /= h;
      mat[j][i]=mat[i][j];
    }
  }

  solvele(mat,coefs,(unsigned int)(CENTER+1));

  for (i=0;i<=CENTER;i++)
    PyMem_RawFree(mat[i]);
  PyMem_RawFree(mat);
  PyMem_RawFree(hcen);
}

/* double forecast_error(unsigned long i0,unsigned long i1) */
/* { */
/*   int i,n; */
/*   double h,error=0.0; */

/*   for (n=i0+(DIM-1)*DELAY;n<i1-STEP;n++) { */
/*     h=coefs[0]; */
/*     for (i=1;i<=CENTER;i++) */
/*       h += coefs[i]*rbf(&series[n],center[i-1]); */
/*     error += (series[n+STEP]-h)*(series[n+STEP]-h); */
/*   } */

/*   return sqrt(error/(i1-i0-STEP-(DIM-1)*DELAY)); */
/* } */

/* void make_cast(FILE *out) */
/* { */
/*   double *cast,new_el; */
/*   int i,n,dim; */

/*   dim=(DIM-1)*DELAY; */
/*   check_alloc(cast=(double*)malloc(sizeof(double)*(dim+1))); */
/*   for (i=0;i<=dim;i++) */
/*     cast[i]=series[LENGTH-1-dim+i]; */

/*   for (n=0;n<CLENGTH;n++) { */
/*     new_el=coefs[0]; */
/*     for (i=1;i<=CENTER;i++) */
/*       new_el += coefs[i]*rbf(&cast[dim],center[i-1]); */
/*     fprintf(out,"%e\n",new_el*interval+min); */
/*     for (i=0;i<dim;i++) */
/*       cast[i]=cast[i+1]; */
/*     cast[dim]=new_el; */
/*   } */
/* } */

static PyObject *
predict(PyObject *self, PyObject *args)
{
  char stdi=1;
  int i,j,cstep;
  double av;
  double prediction;

  PyObject *tuple;
  PyObject *seq;

  if (!PyArg_ParseTuple(args, "O", &tuple))
    return NULL;

  // Input variables
  //
  // LENGTH = ;
  // exclude = ;
  // COLUMN = ;
  // DIM = ;
  // DELAY = ;
  // CENTER = ;
  // setdrift=0;
  // STEP = ;
  // verbosity = ;
  // INSAMPLE = ;
  // stdo=0; outfile="out";

  // -L 1
  MAKECAST = 1; CLENGTH = 1;

  seq = PySequence_Fast(tuple, "argument must be a list or tuple");

  if(!seq)
    return NULL;

  LENGTH = PySequence_Fast_GET_SIZE(seq);

  // series=(double*)get_series(infile,&LENGTH,exclude,COLUMN,verbosity);
  pycheck_alloc(series = (double*) PyMem_RawMalloc(sizeof(double)*LENGTH));

  for(i=0; i<LENGTH; i++) {
    PyObject *item = PySequence_Fast_GET_ITEM(seq, i);
    series[i] = PyFloat_AS_DOUBLE(PyNumber_Float(item));
  }
  Py_DECREF(seq);

  rescale_data(series,LENGTH,&min,&interval);
  variance(series,LENGTH,&av,&varianz);

  if (INSAMPLE > LENGTH)
    INSAMPLE=LENGTH;

  if (CENTER > LENGTH)
    CENTER = LENGTH;

  if (MAKECAST)
    STEP=1;

  pycheck_alloc(coefs=(double*)PyMem_RawMalloc(sizeof(double)*(CENTER+1)));
  pycheck_alloc(center=(double**)PyMem_RawMalloc(sizeof(double*)*CENTER));
  for (i=0;i<CENTER;i++)
    check_alloc(center[i]=(double*)PyMem_RawMalloc(sizeof(double)*DIM));

  cstep=LENGTH-1-(DIM-1)*DELAY;
  for (i=0;i<CENTER;i++)
    for (j=0;j<DIM;j++)
      center[i][j]=series[(DIM-1)*DELAY-j*DELAY+(i*cstep)/(CENTER-1)];

  if (setdrift)
    drift();
  varianz=avdistance();
  make_fit();

  /* if (!stdo) { */
  /*   file=fopen(outfile,"w"); */
  /*   if (verbosity&VER_INPUT) */
  /*     fprintf(stderr,"Opened %s for writing\n",outfile); */
  /*   fprintf(file,"#Center points used:\n"); */
  /*   for (i=0;i<CENTER;i++) { */
  /*     fprintf(file,"#"); */
  /*     for (j=0;j<DIM;j++) */
  /*       fprintf(file," %e",center[i][j]*interval+min); */
  /*     fprintf(file,"\n"); */
  /*   } */
  /*   fprintf(file,"#variance= %e\n",varianz*interval); */
  /*   fprintf(file,"#Coefficients:\n"); */
  /*   fprintf(file,"#%e\n",coefs[0]*interval+min); */
  /*   for (i=1;i<=CENTER;i++) */
  /*     fprintf(file,"#%e\n",coefs[i]*interval); */
  /* } */
  /* else { */
  /*   if (verbosity&VER_INPUT) */
  /*     fprintf(stderr,"Writing to stdout\n"); */
  /*   fprintf(stdout,"#Center points used:\n"); */
  /*   for (i=0;i<CENTER;i++) { */
  /*     fprintf(stdout,"#"); */
  /*     for (j=0;j<DIM;j++) */
  /*       fprintf(stdout," %e",center[i][j]*interval+min); */
  /*     fprintf(stdout,"\n"); */
  /*   } */
  /*   fprintf(stdout,"#variance= %e\n",varianz*interval); */
  /*   fprintf(stdout,"#Coefficients:\n"); */
  /*   fprintf(stdout,"#%e\n",coefs[0]*interval+min); */
  /*   for (i=1;i<=CENTER;i++) */
  /*     fprintf(stdout,"#%e\n",coefs[i]*interval); */
  /* } */
  /* av=sigma=0.0; */
  /* for (i=0;i<INSAMPLE;i++) { */
  /*   av += series[i]; */
  /*   sigma += series[i]*series[i]; */
  /* } */
  /* av /= INSAMPLE; */
  /* sigma=sqrt(fabs(sigma/INSAMPLE-av*av)); */
  /* if (!stdo) */
  /*   fprintf(file,"#insample error= %e\n",forecast_error(0LU,INSAMPLE)/sigma); */
  /* else */
  /*   fprintf(stdout,"#insample error= %e\n",forecast_error(0LU,INSAMPLE)/sigma); */

  /* if (INSAMPLE < LENGTH) { */
  /*   av=sigma=0.0; */
  /*   for (i=INSAMPLE;i<LENGTH;i++) { */
  /*     av += series[i]; */
  /*     sigma += series[i]*series[i]; */
  /*   } */
  /*   av /= (LENGTH-INSAMPLE); */
  /*   sigma=sqrt(fabs(sigma/(LENGTH-INSAMPLE)-av*av)); */
  /*   if (!stdout) */
  /*     fprintf(file,"#out of sample error= %e\n", */
  /*             forecast_error(INSAMPLE,LENGTH)/sigma); */
  /*   else */
  /*     fprintf(stdout,"#out of sample error= %e\n", */
  /*             forecast_error(INSAMPLE,LENGTH)/sigma); */
  /* } */

  /* if (MAKECAST) { */
  /*   if (!stdo) */
  /*     make_cast(file); */
  /*   else */
  /*     make_cast(stdout); */
  /* } */

  /* if (!stdo) */
  /*   fclose(file); */

  /* make_cast() */
  {
    double *cast,new_el;
    int i,dim;

    dim=(DIM-1)*DELAY;
    pycheck_alloc(cast=(double*)PyMem_RawMalloc(sizeof(double)*(dim+1)));
    for (i=0;i<=dim;i++)
      cast[i]=series[LENGTH-1-dim+i];

    new_el=coefs[0];
    for (i=1;i<=CENTER;i++)
      new_el += coefs[i]*rbf(&cast[dim],center[i-1]);

    PyMem_RawFree(cast);

    prediction = new_el*interval+min;
  }

  for (i=0;i<CENTER;i++)
    PyMem_RawFree(center[i]);
  PyMem_RawFree(center);
  PyMem_RawFree(coefs);
  PyMem_RawFree(series);

  return Py_BuildValue("d", prediction);
}

static PyMethodDef RbfMethods[] =
{
     {"predict", predict, METH_VARARGS, WID_STR},
     {NULL, NULL, 0, NULL}
};

static struct PyModuleDef rbfmodule = {
   PyModuleDef_HEAD_INIT,
   "rbf",   /* name of module */
   "This module models the data using a radial basis function (rbf) ansatz.", /* module documentation, may be NULL */
   -1,      /* size of per-interpreter state of the module,
                or -1 if the module keeps state in global variables. */
   RbfMethods
};

PyMODINIT_FUNC
PyInit_rbf(void)
{
    return PyModule_Create(&rbfmodule);
}
