/*
 * Copyright 2015.  TIBCO Software Inc.  ALL RIGHTS RESERVED.
 */
package com.tibco.ems.util;

import java.io.File;
import java.io.IOException;

/**
 * Title:        FileUtils
 * Description:  This is class supports basic file functions.
 * @author A. Kevin Bailey
 * @version 0.6
 */
@SuppressWarnings({"UnusedDeclaration"})
public class FileUtils
{
    public static boolean deleteFile(File file) throws IOException {
        boolean success = false;

        if(file.isDirectory()){
            //directory is empty, then delete it
            if(file.list().length==0){
                success= file.delete();
            }
            else {
                //list all the directory contents
                String files[] = file.list();

                for (String temp : files) {
                //construct the file structure
                File fileDelete = new File(file, temp);
                //recursive delete
                deleteFile(fileDelete);
            }

            //check the directory again, if empty then delete it
            if(file.list().length==0){
                success = file.delete();
            }
        }
        }
        else {
            //if file, then delete it
            success = file.delete();
        }
        return success;
    }

    public static boolean checkDirectoryExists (String path) {
        boolean exists;
        File directory = new File(path);
        exists = directory.exists() || directory.mkdirs();
        return exists;
    }

    public static boolean checkFileExists(String filename) {
        boolean exists = false;
        if (filename == null || filename.isEmpty())
            return false;
        File tempfile = new File(filename);
        if (tempfile.exists())
            exists = true;
        return exists;
    }
}
