/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.maven.plugin.protoc;

import org.apache.maven.model.FileSet;
import org.apache.maven.plugin.AbstractMojo;
import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugins.annotations.LifecyclePhase;
import org.apache.maven.plugins.annotations.Mojo;
import org.apache.maven.plugins.annotations.Parameter;
import org.apache.maven.project.MavenProject;

import java.io.File;

/**
 * Mojo to generate java classes from .proto files using protoc.
 * See package info for examples of use in a maven pom.
 */
@Mojo(name="protoc", defaultPhase = LifecyclePhase.GENERATE_SOURCES)
public class ProtocMojo extends AbstractMojo {

  @Parameter(defaultValue="${project}", readonly=true)
  private MavenProject project;

  @Parameter
  private File[] imports;

  @Parameter(defaultValue="${project.build.directory}/generated-sources/java")
  private File output;

  @Parameter(required=true)
  private FileSet source;

  @Parameter(defaultValue="protoc")
  private String protocCommand;

  @Parameter(required=true)
  private String protocVersion;

  @Parameter(defaultValue =
      "${project.build.directory}/hadoop-maven-plugins-protoc-checksums.json")
  private String checksumPath;

  public void execute() throws MojoExecutionException {
    final ProtocRunner protoc = new ProtocRunner(project, imports, output,
        source, protocCommand, protocVersion, checksumPath, this, false);
    protoc.execute();
  }

}
