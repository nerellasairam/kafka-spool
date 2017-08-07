/**
 * Copyright © 2016 Jeremy Custenborder (jcustenborder@gmail.com)
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
 */
package io.confluent.kafka.connect.source;

import io.confluent.kafka.connect.source.io.DirectoryMonitor;
import io.confluent.kafka.connect.source.io.PollingDirectoryMonitor;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.Session;
import com.jcraft.jsch.SftpException;
import com.jcraft.jsch.ChannelSftp.LsEntry;

import java.io.File;
import java.util.List;
import java.util.Map;
import java.util.Vector;

public class SpoolDirectoryTask extends SourceTask {
  static final Logger log = LoggerFactory.getLogger(SpoolDirectoryTask.class);
  SpoolDirectoryConfig config;
  DirectoryMonitor directoryMonitor;
  private boolean firstTime = false;
  @Override
  public String version() {
    return VersionUtil.getVersion();
  }

  @Override
  public void start(Map<String, String> map) {
    this.config = new SpoolDirectoryConfig(map);
    this.directoryMonitor = new PollingDirectoryMonitor();
    this.directoryMonitor.configure(this.config);
  }

  @Override
  public List<SourceRecord> poll() throws InterruptedException {
    List<SourceRecord> results = this.directoryMonitor.poll();
    if(this.config.sftpUserName() !=null)
    	sftpTransferFromRemoteHost();
    
    while (results.isEmpty()) {   	
    	Thread.sleep(1000);    
      results = this.directoryMonitor.poll(); 
  }
    return results;
  }
 
  public void sftpTransferFromRemoteHost() {

	    String user = this.config.sftpUserName();
	    String password = this.config.sftpPassword();
	    String host = this.config.sftpHostName();
	    int port=Integer.parseInt(this.config.sftpPort());

	    String inputDir = this.config.inputPath() +"/";

	 

	    try
	    {

	        JSch jsch = new JSch();
	        Session session = jsch.getSession(user, host, port);
	        session.setPassword(password);
	        session.setConfig("StrictHostKeyChecking", "no");
	        log.info("Establishing Connection... to host : port "+ host + port);
	        session.connect();
	        log.info("Connection established.");
	        log.info("Creating SFTP Channel.");
	        ChannelSftp sftpChannel = (ChannelSftp) session.openChannel("sftp");
	        sftpChannel.connect();
	        log.info("SFTP Channel created.");
		    sftpChannel.cd(this.config.sftpRemoteDirDoc());
		    Vector<LsEntry> entries = sftpChannel.ls("*.*");
		    Vector<String> fileToBeTransferred = new Vector<String>();
		
		if(!entries.isEmpty()){
	        for (LsEntry entry : entries) 
		    fileToBeTransferred.addElement(entry.getFilename());
	     }
		
		if(!fileToBeTransferred.isEmpty())
		{
	    		log.info("Files to be transfered is not empty" ); 
			for ( String filename : fileToBeTransferred)
			{				
	        	String destFile = inputDir+filename;
	        	log.info("Transfering "+ filename + " To " + destFile) ;
			sftpChannel.get(filename,destFile);
			}
			log.info("Disconnecting from sftp server" ) ;
			sftpChannel.rm("*.*");
			sftpChannel.disconnect();
	        session.disconnect();
		}else {
			log.info("Disconnecting from sftp server" ) ;
			sftpChannel.disconnect();
	        session.disconnect();
		}

	    }
	 
	    catch(JSchException | SftpException e)
	    {
	    	System.out.println(e);
	    }
	}
  @Override
  public void stop() {

  }
}