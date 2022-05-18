package com.thinkbox.md.service;

import java.io.File;

import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;

import lombok.extern.slf4j.Slf4j;

@Service
@Slf4j
public class MainService {

	private final static String USER_HOME = "user.home";
	
	private final String ASYNC_EXECUTOR = "asyncExecutor";

	@Async(ASYNC_EXECUTOR)
	public void cleanupFolders() {
		log.info("Start deleting files");	
		File file = new File(System.getProperty(USER_HOME) + File.separator + "save");
		deleteFiles(file);
		File file1 = new File(System.getProperty(USER_HOME) + File.separator + "create");
		deleteFiles(file1);
		File file2 = new File(System.getProperty(USER_HOME) + File.separator + "enrich");
		deleteFiles(file2);
		File file3 = new File(System.getProperty(USER_HOME) + File.separator + "dbget");
		deleteFiles(file3);
		log.info("Finished deleting files");

	}

	private void deleteFiles(File dirPath) {
		File filesList[] = dirPath.listFiles();
		for (File file : filesList) {
			if (file.isFile()) {
				file.delete();
			} else {
				deleteFiles(file);
			}
		}
	}
}
