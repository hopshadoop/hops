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
#include "configuration.h"
#include "container-executor.h"

#include <errno.h>
#include <fcntl.h>
#include <unistd.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/wait.h>

#define TEST_ROOT "/tmp/test-container-executor"
#define DONT_TOUCH_FILE "dont-touch-me"
#define NM_LOCAL_DIRS       TEST_ROOT "/local-1%" TEST_ROOT "/local-2%" \
               TEST_ROOT "/local-3%" TEST_ROOT "/local-4%" TEST_ROOT "/local-5"
#define NM_LOG_DIRS         TEST_ROOT "/logs/userlogs"
#define ARRAY_SIZE 1000

static char* username = NULL;
static char* yarn_username = NULL;
static char** local_dirs = NULL;
static char** log_dirs = NULL;

/**
 * Run the command using the effective user id.
 * It can't use system, since bash seems to copy the real user id into the
 * effective id.
 */
void run(const char *cmd) {
  fflush(stdout);
  fflush(stderr);
  pid_t child = fork();
  if (child == -1) {
    printf("FAIL: failed to fork - %s\n", strerror(errno));
  } else if (child == 0) {
    char *cmd_copy = strdup(cmd);
    char *ptr;
    int words = 1;
    for(ptr = strchr(cmd_copy, ' ');  ptr; ptr = strchr(ptr+1, ' ')) {
      words += 1;
    }
    char **argv = malloc(sizeof(char *) * (words + 1));
    ptr = strtok(cmd_copy, " ");
    int i = 0;
    argv[i++] = ptr;
    while (ptr != NULL) {
      ptr = strtok(NULL, " ");
      argv[i++] = ptr;
    }
    if (execvp(argv[0], argv) != 0) {
      printf("FAIL: exec failed in child %s - %s\n", cmd, strerror(errno));
      exit(42);
    }
  } else {
    int status = 0;
    if (waitpid(child, &status, 0) <= 0) {
      printf("FAIL: failed waiting for child process %s pid %d - %s\n", 
	     cmd, child, strerror(errno));
      exit(1);
    }
    if (!WIFEXITED(status)) {
      printf("FAIL: process %s pid %d did not exit\n", cmd, child);
      exit(1);
    }
    if (WEXITSTATUS(status) != 0) {
      printf("FAIL: process %s pid %d exited with error status %d\n", cmd, 
	     child, WEXITSTATUS(status));
      exit(1);
    }
  }
}

int write_config_file(char *file_name, int banned) {
  FILE *file;
  file = fopen(file_name, "w");
  if (file == NULL) {
    printf("Failed to open %s.\n", file_name);
    return EXIT_FAILURE;
  }
  if (banned != 0) {
    fprintf(file, "banned.users=bannedUser\n");
    fprintf(file, "min.user.id=500\n");
  } else {
    fprintf(file, "min.user.id=0\n");
  }
  fprintf(file, "allowed.system.users=allowedUser,daemon\n");
  fclose(file);
  return 0;
}

void create_nm_roots(char ** nm_roots) {
  char** nm_root;
  for(nm_root=nm_roots; *nm_root != NULL; ++nm_root) {
    if (mkdir(*nm_root, 0755) != 0) {
      printf("FAIL: Can't create directory %s - %s\n", *nm_root,
             strerror(errno));
      exit(1);
    }
    char buffer[100000];
    sprintf(buffer, "%s/usercache", *nm_root);
    if (mkdir(buffer, 0755) != 0) {
      printf("FAIL: Can't create directory %s - %s\n", buffer,
             strerror(errno));
      exit(1);
    }
  }
}

void check_pid_file(const char* pid_file, pid_t mypid) {
  if(access(pid_file, R_OK) != 0) {
    printf("FAIL: failed to create pid file %s\n", pid_file);
    exit(1);
  }
  int pidfd = open(pid_file, O_RDONLY);
  if (pidfd == -1) {
    printf("FAIL: failed to open pid file %s - %s\n", pid_file, strerror(errno));
    exit(1);
  }

  char pidBuf[100];
  ssize_t bytes = read(pidfd, pidBuf, 100);
  if (bytes == -1) {
    printf("FAIL: failed to read from pid file %s - %s\n", pid_file, strerror(errno));
    exit(1);
  }

  char myPidBuf[33];
  snprintf(myPidBuf, 33, "%d", mypid);
  if (strncmp(pidBuf, myPidBuf, strlen(myPidBuf)) != 0) {
    printf("FAIL: failed to find matching pid in pid file\n");
    printf("FAIL: Expected pid %d : Got %.*s", mypid, (int)bytes, pidBuf);
    exit(1);
  }
}

void test_get_user_directory() {
  char *user_dir = get_user_directory("/tmp", "user");
  char *expected = "/tmp/usercache/user";
  if (strcmp(user_dir, expected) != 0) {
    printf("test_get_user_directory expected %s got %s\n", expected, user_dir);
    exit(1);
  }
  free(user_dir);
}

void test_get_app_directory() {
  char *expected = "/tmp/usercache/user/appcache/app_200906101234_0001";
  char *app_dir = (char *) get_app_directory("/tmp", "user",
      "app_200906101234_0001");
  if (strcmp(app_dir, expected) != 0) {
    printf("test_get_app_directory expected %s got %s\n", expected, app_dir);
    exit(1);
  }
  free(app_dir);
}

void test_get_container_directory() {
  char *container_dir = get_container_work_directory("/tmp", "owen", "app_1",
						 "container_1");
  char *expected = "/tmp/usercache/owen/appcache/app_1/container_1";
  if (strcmp(container_dir, expected) != 0) {
    printf("Fail get_container_work_directory got %s expected %s\n",
	   container_dir, expected);
    exit(1);
  }
  free(container_dir);
}

void test_get_container_launcher_file() {
  char *expected_file = ("/tmp/usercache/user/appcache/app_200906101234_0001"
			 "/launch_container.sh");
  char *app_dir = get_app_directory("/tmp", "user",
                                    "app_200906101234_0001");
  char *container_file =  get_container_launcher_file(app_dir);
  if (strcmp(container_file, expected_file) != 0) {
    printf("failure to match expected container file %s vs %s\n", container_file,
           expected_file);
    exit(1);
  }
  free(app_dir);
  free(container_file);
}

void test_get_app_log_dir() {
  char *expected = TEST_ROOT "/logs/userlogs/app_200906101234_0001";
  char *logdir = get_app_log_directory(TEST_ROOT "/logs/userlogs","app_200906101234_0001");
  if (strcmp(logdir, expected) != 0) {
    printf("Fail get_app_log_dir got %s expected %s\n", logdir, expected);
    exit(1);
  }
  free(logdir);
}

void test_check_user() {
  printf("\nTesting test_check_user\n");
  struct passwd *user = check_user(username);
  if (user == NULL) {
    printf("FAIL: failed check for user %s\n", username);
    exit(1);
  }
  free(user);
  if (check_user("lp") != NULL) {
    printf("FAIL: failed check for system user lp\n");
    exit(1);
  }
  if (check_user("root") != NULL) {
    printf("FAIL: failed check for system user root\n");
    exit(1);
  }
  if (check_user("daemon") == NULL) {
    printf("FAIL: failed check for whitelisted system user daemon\n");
    exit(1);
  }
}

void test_resolve_config_path() {
  printf("\nTesting resolve_config_path\n");
  if (strcmp(resolve_config_path("/bin/ls", NULL), "/bin/ls") != 0) {
    printf("FAIL: failed to resolve config_name on an absolute path name: /bin/ls\n");
    exit(1);
  }
  if (strcmp(resolve_config_path("../bin/ls", "/bin/ls"), "/bin/ls") != 0) {
    printf("FAIL: failed to resolve config_name on a relative path name: ../bin/ls (relative to /bin/ls)");
    exit(1);
  }
}

void test_check_configuration_permissions() {
  printf("\nTesting check_configuration_permissions\n");
  if (check_configuration_permissions("/etc/passwd") != 0) {
    printf("FAIL: failed permission check on /etc/passwd\n");
    exit(1);
  }
  if (check_configuration_permissions(TEST_ROOT) == 0) {
    printf("FAIL: failed permission check on %s\n", TEST_ROOT);
    exit(1);
  }
}

void test_delete_container() {
  if (initialize_user(yarn_username, local_dirs)) {
    printf("FAIL: failed to initialize user %s\n", yarn_username);
    exit(1);
  }
  char* app_dir = get_app_directory(TEST_ROOT "/local-2", yarn_username, "app_1");
  char* dont_touch = get_app_directory(TEST_ROOT "/local-2", yarn_username, 
                                       DONT_TOUCH_FILE);
  char* container_dir = get_container_work_directory(TEST_ROOT "/local-2", 
					      yarn_username, "app_1", "container_1");
  char buffer[100000];
  sprintf(buffer, "mkdir -p %s/who/let/the/dogs/out/who/who", container_dir);
  run(buffer);
  sprintf(buffer, "touch %s", dont_touch);
  run(buffer);

  // soft link to the canary file from the container directory
  sprintf(buffer, "ln -s %s %s/who/softlink", dont_touch, container_dir);
  run(buffer);
  // hard link to the canary file from the container directory
  sprintf(buffer, "ln %s %s/who/hardlink", dont_touch, container_dir);
  run(buffer);
  // create a dot file in the container directory
  sprintf(buffer, "touch %s/who/let/.dotfile", container_dir);
  run(buffer);
  // create a no permission file
  sprintf(buffer, "touch %s/who/let/protect", container_dir);
  run(buffer);
  sprintf(buffer, "chmod 000 %s/who/let/protect", container_dir);
  run(buffer);
  // create a no permission directory
  sprintf(buffer, "chmod 000 %s/who/let", container_dir);
  run(buffer);

  // delete container directory
  char * dirs[] = {app_dir, 0};
  int ret = delete_as_user(yarn_username, "container_1" , dirs);
  if (ret != 0) {
    printf("FAIL: return code from delete_as_user is %d\n", ret);
    exit(1);
  }

  // check to make sure the container directory is gone
  if (access(container_dir, R_OK) == 0) {
    printf("FAIL: failed to delete the directory - %s\n", container_dir);
    exit(1);
  }
  // check to make sure the app directory is not gone
  if (access(app_dir, R_OK) != 0) {
    printf("FAIL: accidently deleted the directory - %s\n", app_dir);
    exit(1);
  }
  // but that the canary is not gone
  if (access(dont_touch, R_OK) != 0) {
    printf("FAIL: accidently deleted file %s\n", dont_touch);
    exit(1);
  }
  sprintf(buffer, "chmod -R 700 %s", app_dir);
  run(buffer);
  sprintf(buffer, "rm -fr %s", app_dir);
  run(buffer);
  free(app_dir);
  free(container_dir);
  free(dont_touch);
}

void test_delete_app() {
  char* app_dir = get_app_directory(TEST_ROOT "/local-2", yarn_username, "app_2");
  char* dont_touch = get_app_directory(TEST_ROOT "/local-2", yarn_username, 
                                       DONT_TOUCH_FILE);
  char* container_dir = get_container_work_directory(TEST_ROOT "/local-2", 
					      yarn_username, "app_2", "container_1");
  char buffer[100000];
  sprintf(buffer, "mkdir -p %s/who/let/the/dogs/out/who/who", container_dir);
  run(buffer);
  sprintf(buffer, "touch %s", dont_touch);
  run(buffer);

  // soft link to the canary file from the container directory
  sprintf(buffer, "ln -s %s %s/who/softlink", dont_touch, container_dir);
  run(buffer);
  // hard link to the canary file from the container directory
  sprintf(buffer, "ln %s %s/who/hardlink", dont_touch, container_dir);
  run(buffer);
  // create a dot file in the container directory
  sprintf(buffer, "touch %s/who/let/.dotfile", container_dir);
  run(buffer);
  // create a no permission file
  sprintf(buffer, "touch %s/who/let/protect", container_dir);
  run(buffer);
  sprintf(buffer, "chmod 000 %s/who/let/protect", container_dir);
  run(buffer);
  // create a no permission directory
  sprintf(buffer, "chmod 000 %s/who/let", container_dir);
  run(buffer);

  // delete container directory
  int ret = delete_as_user(yarn_username, app_dir, NULL);
  if (ret != 0) {
    printf("FAIL: return code from delete_as_user is %d\n", ret);
    exit(1);
  }

  // check to make sure the container directory is gone
  if (access(container_dir, R_OK) == 0) {
    printf("FAIL: failed to delete the directory - %s\n", container_dir);
    exit(1);
  }
  // check to make sure the app directory is gone
  if (access(app_dir, R_OK) == 0) {
    printf("FAIL: didn't delete the directory - %s\n", app_dir);
    exit(1);
  }
  // but that the canary is not gone
  if (access(dont_touch, R_OK) != 0) {
    printf("FAIL: accidently deleted file %s\n", dont_touch);
    exit(1);
  }
  free(app_dir);
  free(container_dir);
  free(dont_touch);
}


void test_delete_user() {
  printf("\nTesting delete_user\n");
  char* app_dir = get_app_directory(TEST_ROOT "/local-1", yarn_username, "app_3");
  if (mkdirs(app_dir, 0700) != 0) {
    exit(1);
  }

  char buffer[100000];
  sprintf(buffer, "%s/test.cfg", app_dir);
  if (write_config_file(buffer, 1) != 0) {
    exit(1);
  }

  char * dirs[] = {buffer, 0};
  int ret = delete_as_user(yarn_username, "file1" , dirs);
  if (ret == 0) {
    printf("FAIL: if baseDir is a file, delete_as_user should fail if a subdir is also passed\n");
    exit(1);
  }

  // Pass a file to delete_as_user in the baseDirs parameter. The file should
  // be deleted.
  ret = delete_as_user(yarn_username, "" , dirs);
  if (ret != 0) {
    printf("FAIL: delete_as_user could not delete baseDir when baseDir is a file: return code is %d\n", ret);
    exit(1);
  }

  sprintf(buffer, "%s", app_dir);
  char missing_dir[20];
  strcpy(missing_dir, "/some/missing/dir");
  char * dirs_with_missing[] = {missing_dir, buffer, 0};
  ret = delete_as_user(yarn_username, "" , dirs_with_missing);
  printf("%d" , ret);
  if (access(buffer, R_OK) == 0) {
    printf("FAIL: directory not deleted\n");
    exit(1);
  }

  sprintf(buffer, "%s/local-1/usercache/%s", TEST_ROOT, yarn_username);
  if (access(buffer, R_OK) != 0) {
    printf("FAIL: directory missing before test\n");
    exit(1);
  }
  if (delete_as_user(yarn_username, buffer, NULL) != 0) {
    exit(1);
  }
  if (access(buffer, R_OK) == 0) {
    printf("FAIL: directory not deleted\n");
    exit(1);
  }
  if (access(TEST_ROOT "/local-1", R_OK) != 0) {
    printf("FAIL: local-1 directory does not exist\n");
    exit(1);
  }
  free(app_dir);
}

void run_test_in_child(const char* test_name, void (*func)()) {
  printf("\nRunning test %s in child process\n", test_name);
  fflush(stdout);
  fflush(stderr);
  pid_t child = fork();
  if (child == -1) {
    printf("FAIL: fork failed\n");
    exit(1);
  } else if (child == 0) {
    func();
    exit(0);
  } else {
    int status = 0;
    if (waitpid(child, &status, 0) == -1) {
      printf("FAIL: waitpid %d failed - %s\n", child, strerror(errno));
      exit(1);
    }
    if (!WIFEXITED(status)) {
      printf("FAIL: child %d didn't exit - %d\n", child, status);
      exit(1);
    }
    if (WEXITSTATUS(status) != 0) {
      printf("FAIL: child %d exited with bad status %d\n",
	     child, WEXITSTATUS(status));
      exit(1);
    }
  }
}

void test_signal_container_group() {
  printf("\nTesting group signal_container\n");
  fflush(stdout);
  fflush(stderr);
  pid_t child = fork();
  if (child == -1) {
    printf("FAIL: fork failed\n");
    exit(1);
  } else if (child == 0) {
    setpgid(0,0);
    if (change_user(user_detail->pw_uid, user_detail->pw_gid) != 0) {
      exit(1);
    }
    sleep(3600);
    exit(0);
  }
  printf("Child container launched as %d\n", child);
  // there's a race condition for child calling change_user and us 
  // calling signal_container_as_user, hence sleeping
  sleep(3);
  if (signal_container_as_user(yarn_username, child, SIGKILL) != 0) {
    exit(1);
  }
  int status = 0;
  if (waitpid(child, &status, 0) == -1) {
    printf("FAIL: waitpid failed - %s\n", strerror(errno));
    exit(1);
  }
  if (!WIFSIGNALED(status)) {
    printf("FAIL: child wasn't signalled - %d\n", status);
    exit(1);
  }
  if (WTERMSIG(status) != SIGKILL) {
    printf("FAIL: child was killed with %d instead of %d\n", 
	   WTERMSIG(status), SIGKILL);
    exit(1);
  }
}

void test_init_app() {
  printf("\nTesting init app\n");
  if (seteuid(0) != 0) {
    printf("FAIL: seteuid to root failed - %s\n", strerror(errno));
    exit(1);
  }
  FILE* creds = fopen(TEST_ROOT "/creds.txt", "w");
  if (creds == NULL) {
    printf("FAIL: failed to create credentials file - %s\n", strerror(errno));
    exit(1);
  }
  if (fprintf(creds, "secret key\n") < 0) {
    printf("FAIL: fprintf failed - %s\n", strerror(errno));
    exit(1);
  }
  if (fclose(creds) != 0) {
    printf("FAIL: fclose failed - %s\n", strerror(errno));
    exit(1);
  }
  FILE* job_xml = fopen(TEST_ROOT "/job.xml", "w");
  if (job_xml == NULL) {
    printf("FAIL: failed to create job file - %s\n", strerror(errno));
    exit(1);
  }
  if (fprintf(job_xml, "<jobconf/>\n") < 0) {
    printf("FAIL: fprintf failed - %s\n", strerror(errno));
    exit(1);
  }
  if (fclose(job_xml) != 0) {
    printf("FAIL: fclose failed - %s\n", strerror(errno));
    exit(1);
  }
  if (seteuid(user_detail->pw_uid) != 0) {
    printf("FAIL: failed to seteuid back to user - %s\n", strerror(errno));
    exit(1);
  }
  fflush(stdout);
  fflush(stderr);
  pid_t child = fork();
  if (child == -1) {
    printf("FAIL: failed to fork process for init_app - %s\n", 
	   strerror(errno));
    exit(1);
  } else if (child == 0) {
    char *final_pgm[] = {"touch", "my-touch-file", 0};
    if (initialize_app(yarn_username, "app_4", TEST_ROOT "/creds.txt",
                       local_dirs, log_dirs, final_pgm) != 0) {
      printf("FAIL: failed in child\n");
      exit(42);
    }
    // should never return
    exit(1);
  }
  int status = 0;
  if (waitpid(child, &status, 0) <= 0) {
    printf("FAIL: failed waiting for process %d - %s\n", child, 
	   strerror(errno));
    exit(1);
  }
  if (access(TEST_ROOT "/logs/userlogs/app_4", R_OK) != 0) {
    printf("FAIL: failed to create app log directory\n");
    exit(1);
  }
  char* app_dir = get_app_directory(TEST_ROOT "/local-1", yarn_username, "app_4");
  if (access(app_dir, R_OK) != 0) {
    printf("FAIL: failed to create app directory %s\n", app_dir);
    exit(1);
  }
  char buffer[100000];
  sprintf(buffer, "%s/creds.txt", app_dir);
  if (access(buffer, R_OK) != 0) {
    printf("FAIL: failed to create credentials %s\n", buffer);
    exit(1);
  }
  sprintf(buffer, "%s/my-touch-file", app_dir);
  if (access(buffer, R_OK) != 0) {
    printf("FAIL: failed to create touch file %s\n", buffer);
    exit(1);
  }
  free(app_dir);
  app_dir = get_app_log_directory(TEST_ROOT "/logs/userlogs","app_4");
  if (access(app_dir, R_OK) != 0) {
    printf("FAIL: failed to create app log directory %s\n", app_dir);
    exit(1);
  }
  free(app_dir);
}

void test_run_container() {
  printf("\nTesting run container\n");
  if (seteuid(0) != 0) {
    printf("FAIL: seteuid to root failed - %s\n", strerror(errno));
    exit(1);
  }
  FILE* creds = fopen(TEST_ROOT "/creds.txt", "w");
  if (creds == NULL) {
    printf("FAIL: failed to create credentials file - %s\n", strerror(errno));
    exit(1);
  }
  if (fprintf(creds, "secret key\n") < 0) {
    printf("FAIL: fprintf failed - %s\n", strerror(errno));
    exit(1);
  }
  if (fclose(creds) != 0) {
    printf("FAIL: fclose failed - %s\n", strerror(errno));
    exit(1);
  }

  char * cgroups_pids[] = { TEST_ROOT "/cgroups-pid1.txt", TEST_ROOT "/cgroups-pid2.txt", 0 };
  close(creat(cgroups_pids[0], O_RDWR));
  close(creat(cgroups_pids[1], O_RDWR));

  const char* script_name = TEST_ROOT "/container-script";
  FILE* script = fopen(script_name, "w");
  if (script == NULL) {
    printf("FAIL: failed to create script file - %s\n", strerror(errno));
    exit(1);
  }
  if (seteuid(user_detail->pw_uid) != 0) {
    printf("FAIL: failed to seteuid back to user - %s\n", strerror(errno));
    exit(1);
  }
  if (fprintf(script, "#!/bin/bash\n"
                     "touch foobar\n"
                     "exit 0") < 0) {
    printf("FAIL: fprintf failed - %s\n", strerror(errno));
    exit(1);
  }
  if (fclose(script) != 0) {
    printf("FAIL: fclose failed - %s\n", strerror(errno));
    exit(1);
  }
  fflush(stdout);
  fflush(stderr);
  char* container_dir = get_container_work_directory(TEST_ROOT "/local-1", 
					      yarn_username, "app_4", "container_1");
  const char * pid_file = TEST_ROOT "/pid.txt";

  pid_t child = fork();
  if (child == -1) {
    printf("FAIL: failed to fork process for init_app - %s\n", 
	   strerror(errno));
    exit(1);
  } else if (child == 0) {
    if (launch_container_as_user(yarn_username, "app_4", "container_1", 
          container_dir, script_name, TEST_ROOT "/creds.txt", pid_file,
          local_dirs, log_dirs,
          "cgroups", cgroups_pids) != 0) {
      printf("FAIL: failed in child\n");
      exit(42);
    }
    // should never return
    exit(1);
  }
  int status = 0;
  if (waitpid(child, &status, 0) <= 0) {
    printf("FAIL: failed waiting for process %d - %s\n", child, 
	   strerror(errno));
    exit(1);
  }
  if (access(TEST_ROOT "/logs/userlogs/app_4/container_1", R_OK) != 0) {
    printf("FAIL: failed to create container log directory\n");
    exit(1);
  }
  if (access(container_dir, R_OK) != 0) {
    printf("FAIL: failed to create container directory %s\n", container_dir);
    exit(1);
  }
  char buffer[100000];
  sprintf(buffer, "%s/foobar", container_dir);
  if (access(buffer, R_OK) != 0) {
    printf("FAIL: failed to create touch file %s\n", buffer);
    exit(1);
  }
  free(container_dir);
  container_dir = get_app_log_directory(TEST_ROOT "/logs/userlogs", "app_4/container_1");
  if (access(container_dir, R_OK) != 0) {
    printf("FAIL: failed to create app log directory %s\n", container_dir);
    exit(1);
  }
  free(container_dir);

  if (seteuid(0) != 0) {
    printf("FAIL: seteuid to root failed - %s\n", strerror(errno));
    exit(1);
  }

  check_pid_file(pid_file, child);
  check_pid_file(cgroups_pids[0], child);
  check_pid_file(cgroups_pids[1], child);
}

// This test is expected to be executed either by a regular
// user or by root. If executed by a regular user it doesn't
// test all the functions that would depend on changing the
// effective user id. If executed by a super-user everything
// gets tested. Here are different ways of execing the test binary:
// 1. regular user assuming user == yarn user
//    $ test-container-executor     
// 2. regular user with a given yarn user
//    $ test-container-executor yarn_user
// 3. super user with a given user and assuming user == yarn user
//    # test-container-executor user
// 4. super user with a given user and a given yarn user
//    # test-container-executor user yarn_user
int main(int argc, char **argv) {
  LOGFILE = stdout;
  ERRORFILE = stderr;

  // clean up any junk from previous run
  if (system("chmod -R u=rwx " TEST_ROOT "; rm -fr " TEST_ROOT)) {
    exit(1);
  }
  
  if (mkdirs(TEST_ROOT "/logs/userlogs", 0755) != 0) {
    exit(1);
  }
  
  if (write_config_file(TEST_ROOT "/test.cfg", 1) != 0) {
    exit(1);
  }
  read_config(TEST_ROOT "/test.cfg");

  local_dirs = extract_values(strdup(NM_LOCAL_DIRS));
  log_dirs = extract_values(strdup(NM_LOG_DIRS));

  create_nm_roots(local_dirs);

  // See the description above of various ways this test
  // can be executed in order to understand the following logic
  char* current_username = strdup(getpwuid(getuid())->pw_name);
  if (getuid() == 0 && (argc == 2 || argc == 3)) {
    username = argv[1];
    yarn_username = (argc == 3) ? argv[2] : argv[1];
  } else {
    username = current_username;
    yarn_username = (argc == 2) ? argv[1] : current_username;
  }
  set_nm_uid(geteuid(), getegid());

  if (set_user(username)) {
    exit(1);
  }

  printf("\nStarting tests\n");

  printf("\nTesting resolve_config_path()\n");
  test_resolve_config_path();

  printf("\nTesting get_user_directory()\n");
  test_get_user_directory();

  printf("\nTesting get_app_directory()\n");
  test_get_app_directory();

  printf("\nTesting get_container_directory()\n");
  test_get_container_directory();

  printf("\nTesting get_container_launcher_file()\n");
  test_get_container_launcher_file();

  printf("\nTesting get_app_log_dir()\n");
  test_get_app_log_dir();

  test_check_configuration_permissions();

  printf("\nTesting delete_container()\n");
  test_delete_container();

  printf("\nTesting delete_app()\n");
  test_delete_app();

  test_check_user();

  // the tests that change user need to be run in a subshell, so that
  // when they change user they don't give up our privs
  run_test_in_child("test_signal_container_group", test_signal_container_group);

  // init app and run container can't be run if you aren't testing as root
  if (getuid() == 0) {
    // these tests do internal forks so that the change_owner and execs
    // don't mess up our process.
    test_init_app();
    test_run_container();
  }

  seteuid(0);
  // test_delete_user must run as root since that's how we use the delete_as_user
  test_delete_user();
  free_configurations();

  printf("\nTrying banned default user()\n");
  if (write_config_file(TEST_ROOT "/test.cfg", 0) != 0) {
    exit(1);
  }

  read_config(TEST_ROOT "/test.cfg");
  username = "bin";
  test_check_user();

  run("rm -fr " TEST_ROOT);
  printf("\nFinished tests\n");

  free(current_username);
  free_configurations();
  return 0;
}
