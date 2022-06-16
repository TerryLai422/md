package com.thinkbox.md.service;

import java.io.File;
import java.util.Arrays;
import java.util.List;

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
		List<String> list = Arrays.asList("save", "create", "enrich", "dbget", "consolidate");
		

		list.forEach(x -> {
			deleteFiles(new File(System.getProperty(USER_HOME) + File.separator + x));
			log.info("Deleted: " + x);
		});

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
