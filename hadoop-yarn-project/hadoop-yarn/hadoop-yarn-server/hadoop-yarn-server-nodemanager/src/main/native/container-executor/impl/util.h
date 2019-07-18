/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#ifndef __YARN_POSIX_CONTAINER_EXECUTOR_UTIL_H__
#define __YARN_POSIX_CONTAINER_EXECUTOR_UTIL_H__

/** Define a platform-independent constant instead of using PATH_MAX, set to 4K */
#define EXECUTOR_PATH_MAX 4096

#include <stdio.h>
#include <stdlib.h>

enum errorcodes {
  INVALID_ARGUMENT_NUMBER = 1,
  //INVALID_USER_NAME 2
  INVALID_COMMAND_PROVIDED = 3,
  // SUPER_USER_NOT_ALLOWED_TO_RUN_TASKS (NOT USED) 4
  INVALID_NM_ROOT_DIRS = 5,
  SETUID_OPER_FAILED, //6
  UNABLE_TO_EXECUTE_CONTAINER_SCRIPT, //7
  UNABLE_TO_SIGNAL_CONTAINER, //8
  INVALID_CONTAINER_PID, //9
  // ERROR_RESOLVING_FILE_PATH (NOT_USED) 10
  // RELATIVE_PATH_COMPONENTS_IN_FILE_PATH (NOT USED) 11
  // UNABLE_TO_STAT_FILE (NOT USED) 12
  // FILE_NOT_OWNED_BY_ROOT (NOT USED) 13
  // PREPARE_CONTAINER_DIRECTORIES_FAILED (NOT USED) 14
  // INITIALIZE_CONTAINER_FAILED (NOT USED) 15
  // PREPARE_CONTAINER_LOGS_FAILED (NOT USED) 16
  // INVALID_LOG_DIR (NOT USED) 17
  OUT_OF_MEMORY = 18,
  // INITIALIZE_DISTCACHEFILE_FAILED (NOT USED) 19
  INITIALIZE_USER_FAILED = 20,
  PATH_TO_DELETE_IS_NULL, //21
  INVALID_CONTAINER_EXEC_PERMISSIONS, //22
  // PREPARE_JOB_LOGS_FAILED (NOT USED) 23
  INVALID_CONFIG_FILE = 24,
  SETSID_OPER_FAILED = 25,
  WRITE_PIDFILE_FAILED = 26,
  WRITE_CGROUP_FAILED = 27,
  TRAFFIC_CONTROL_EXECUTION_FAILED = 28,
  DOCKER_RUN_FAILED = 29,
  ERROR_OPENING_DOCKER_FILE = 30,
  ERROR_READING_DOCKER_FILE = 31,
  FEATURE_DISABLED = 32,
  COULD_NOT_CREATE_SCRIPT_COPY = 33,
  COULD_NOT_CREATE_CREDENTIALS_FILE = 34,
  COULD_NOT_CREATE_WORK_DIRECTORIES = 35,
  COULD_NOT_CREATE_APP_LOG_DIRECTORIES = 36,
  COULD_NOT_CREATE_TMP_DIRECTORIES = 37,
  ERROR_CREATE_CONTAINER_DIRECTORIES_ARGUMENTS = 38,
  ERROR_SANITIZING_DOCKER_COMMAND = 39,
  DOCKER_IMAGE_INVALID = 40,
  // DOCKER_CONTAINER_NAME_INVALID = 41, (NOT USED)
  ERROR_COMPILING_REGEX = 42,
  INVALID_CONTAINER_ID = 43
};

/* Macros for min/max. */
#ifndef MIN
#define MIN(a,b) (((a)<(b))?(a):(b))
#endif /* MIN */
#ifndef MAX
#define MAX(a,b) (((a)>(b))?(a):(b))
#endif  /* MAX */

// the log file for messages
extern FILE *LOGFILE;
// the log file for error messages
extern FILE *ERRORFILE;
/**
 * Function to split the given string using '%' as the separator. It's
 * up to the caller to free the memory for the returned array. Use the
 * free_values function to free the allocated memory.
 *
 * @param str the string to split
 *
 * @return an array of strings
 */
char** split(char *str);

/**
 * Function to split the given string using the delimiter specified. It's
 * up to the caller to free the memory for the returned array. Use the
 * free_values function to free the allocated memory.
 *
 * @param str the string to split
 * @param delimiter the delimiter to use
 *
 * @return an array of strings
 */
char** split_delimiter(char *value, const char *delimiter);

/**
 * Function to free an array of strings.
 *
 * @param values the array to free
 *
 */
void free_values(char **values);

/**
 * Trim whitespace from beginning and end. The returned string has to be freed
 * by the caller.
 *
 * @param input    Input string that needs to be trimmed
 *
 * @return the trimmed string allocated with malloc
*/
char* trim(const char *input);

/**
 * Run a regex to check if the provided input matches against it
 * @param regex_str Regex to run
 * @param input String to run the regex against
 * @return 0 on match, non-zero on no match
 */
int execute_regex_match(const char *regex_str, const char *input);

/**
 * Helper function to escape single-quotes in a string. The assumption is that the string passed will be enclosed in
 * single quotes and passed to bash for a command invocation.
 * @param str The string in which to esacpe single quotes
 * @return String with single quotes escaped, must be freed by the user.
 */
char* escape_single_quote(const char *str);

/**
 * Helper function to quote the argument to a parameter and then append it to the provided string.
 * @param str Buffer to which the param'arg' string must be appended
 * @param size Size of the buffer
 * @param param Param name
 * @param arg Argument to be quoted
 */
void quote_and_append_arg(char **str, size_t *size, const char* param, const char *arg);
#define QUOTE_AND_APPEND_ARG_GROWTH (1024) // how much to increase str buffer by if needed

/**
 * Helper function to allocate and clear a block of memory. It'll call exit if the allocation fails.
 * @param num Num of elements to be allocated
 * @param size Size of each element
 * @return Pointer to the allocated memory, must be freed by the user
 */
inline void* alloc_and_clear_memory(size_t num, size_t size) {
  void *ret = calloc(num, size);
  if (ret == NULL) {
    printf("Could not allocate memory, exiting\n");
    exit(OUT_OF_MEMORY);
  }
  return ret;
}

#endif
