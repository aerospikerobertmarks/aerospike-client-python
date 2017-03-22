/*******************************************************************************
 * Copyright 2013-2016 Aerospike, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 ******************************************************************************/

#include <Python.h>
#include <stdbool.h>

#include <aerospike/as_error.h>
#include <aerospike/aerospike.h>
#include "client.h"
#include "conversions.h"
#include "exceptions.h"
#include "policy.h"

static PyObject* 
AerospikeClient_TruncateInvoke(
	AerospikeClient* self, char* namespace, char* set,
	uint64_t nanos, PyObject* py_policy, as_error* err)
{
	as_policy_info info_policy;
	as_policy_info* info_policy_p = NULL;

	pyobject_to_policy_info(err, py_policy, &info_policy, &info_policy_p, &self->as->config.policies.info);

	if (err->code != AEROSPIKE_OK) {
		as_error_update(err, AEROSPIKE_ERR_CLIENT, "Incorrect Policy");
		goto CLEANUP;
	}

	aerospike_truncate(self->as, err, info_policy_p, namespace, set, nanos);

CLEANUP:

	if (err->code != AEROSPIKE_OK) {
		PyObject * py_err = NULL;
		error_to_pyobject(err, &py_err);
		PyObject *exception_type = raise_exception(err);
		PyErr_SetObject(exception_type, py_err);
		Py_DECREF(py_err);
		return NULL;
	}
	return PyLong_FromLong(0);
}

PyObject* 
AerospikeClient_Truncate(AerospikeClient * self, PyObject * args, PyObject * kwds)
{
	PyObject* py_set = NULL;
	PyObject* py_ns = NULL;
	PyObject* py_nanos = NULL;
	PyObject* py_policy = NULL;
	PyObject* py_ustr = NULL;
	bool err_occurred = false;
	long long temp_long;
	as_error err;
	uint64_t nanos;
	char* namespace = NULL;
	char* set = NULL;

	as_error_init(&err);

	static char* kwlist[] = {"namespace", "set", "nanos", "policy", NULL};

	if (PyArg_ParseTupleAndKeywords(args, kwds, "OOO|O:truncate", kwlist,
				&py_ns, &py_set, &py_nanos, &py_policy) == false) {
		return NULL;
	}

	if (PyString_Check(py_ns)) {
		namespace = strdup(PyString_AsString(py_ns));
	} else if (PyUnicode_Check(py_ns)) {
		py_ns = PyUnicode_AsUTF8String(py_ns);
		namespace = strdup(PyBytes_AsString(py_ustr));
		Py_DECREF(py_ustr);
	} else {
		err_occurred = true;
   		PyErr_SetString(PyExc_TypeError, "Namespace must be unicode or string type");
		goto CLEANUP;
	}


	if (PyString_Check(py_set)) {
		set = strdup(PyString_AsString(py_set));
	} else if (PyUnicode_Check(py_set)) {
		py_set = PyUnicode_AsUTF8String(py_set);
		set = strdup(PyBytes_AsString(py_ustr));
		Py_DECREF(py_ustr);
	} else {
		// Do param error here
		err_occurred = true;
   		PyErr_SetString(PyExc_TypeError, "Set must be unicode or string type");
		goto CLEANUP;
	}

	if PyLong_Check(py_nanos) {

		temp_long = PyLong_AsLongLong(py_nanos);
		if (temp_long < 0) {
			err_occurred = true;
			if (PyErr_Occurred()) {
	   			PyErr_SetString(PyExc_OverflowError, "Nanoseconds value out of range for long");
			} else {
	   			PyErr_SetString(PyExc_TypeError, "Nanoseconds must be a positive value");
	   		}
			goto CLEANUP;
		}
		nanos = PyLong_AsUnsignedLong(py_nanos);
		if (PyErr_Occurred()) {
			PyErr_SetString(PyExc_OverflowError, "Nanoseconds value too large");
			err_occurred = true;
			goto CLEANUP;
		}
	} else if PyInt_Check(py_nanos) {
		long tempInt;
		tempInt = PyInt_AsLong(py_nanos);
		if (tempInt == -1 && PyErr_Occurred()) {
			err_occurred = true;
			goto CLEANUP;
		}
		if (tempInt < 0) {
			err_occurred = true;
	   		PyErr_SetString(PyExc_TypeError, "Nanoseconds value must be a positive value");
			goto CLEANUP;
		}
		nanos = (uint64_t)tempInt;
	} else {
		err_occurred = true;
   		PyErr_SetString(PyExc_TypeError, "Nanoseconds must be a long type");
		goto CLEANUP;
	}

//
//	AerospikeClient* self, char* namespace, char* set,
//	uint64_t nanos, PyObject* py_policy, as_error* err)
	AerospikeClient_TruncateInvoke(self, namespace, set, nanos, py_policy, &err);


CLEANUP:
	if (namespace) {
		free(namespace);
	}
	if (set) {
		free(set);
	}
	if (err_occurred) {
		return NULL;
	}
	if (err.code != AEROSPIKE_OK) {
		PyObject * py_err = NULL;
		error_to_pyobject(&err, &py_err);
		PyObject *exception_type = raise_exception(&err);

		PyErr_SetObject(exception_type, py_err);
		Py_DECREF(py_err);

		return NULL;
	}

	return PyLong_FromLong(0);

}
