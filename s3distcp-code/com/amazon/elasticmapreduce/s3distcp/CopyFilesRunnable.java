package com.amazon.elasticmapreduce.s3distcp;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.common.Abortable;
import org.apache.hadoop.io.Text;

class CopyFilesRunnable
  implements Runnable
{
  private static final Log LOG = LogFactory.getLog(CopyFilesRunnable.class);
  private final List<FileInfo> inputFiles;
  private final Path outputFile;
  private final CopyFilesReducer reducer;
  
  public CopyFilesRunnable(CopyFilesReducer reducer, List<FileInfo> inputFiles, Path outputFile)
  {
    this.inputFiles = inputFiles;
    this.outputFile = outputFile;
    this.reducer = reducer;
    LOG.info("Creating CopyFilesRunnable: " + outputFile);
  }
  
  public void run()
  {
    try
    {
      mergeAndCopyFiles();
    }
    catch (Exception e)
    {
      LOG.error("Error downloading input files. Not marking as committed", e);
      return;
    }
    reducer.markFilesAsCommitted(inputFiles);
    if (reducer.shouldDeleteOnSuccess()) {
      deleteOnSuccess();
    }
  }
  
  private long copyStream(InputStream inputStream, OutputStream outputStream)
    throws IOException
  {
    long bytesCopied = 0L;
    try
    {
      byte[] buffer = new byte[reducer.getBufferSize()];
      int len;
      while ((len = inputStream.read(buffer)) > 0)
      {
        outputStream.write(buffer, 0, len);
        reducer.progress();
        bytesCopied += len;
      }
    }
    catch (Exception e)
    {
      throw new IOException("Exception raised while copying data file", e);
    }
    return bytesCopied;
  }
  
  private boolean deleteWithRetries(Path file, boolean recursive)
    throws IOException
  {
    int retriesRemaining = reducer.getNumTransferRetries();
    for (;;)
    {
      try
      {
        FileSystem outFs = file.getFileSystem(reducer.getConf());
        return outFs.delete(file, recursive);
      }
      catch (IOException e)
      {
        retriesRemaining--;
        LOG.warn("Exception raised while attempting to delete=" + file + " numRetriesRemaining=" + retriesRemaining, e);
        if (retriesRemaining <= 0) {
          throw e;
        }
      }
    }
  }
  
  private void deleteOnSuccess()
  {
    for (FileInfo inputFile : inputFiles)
    {
      LOG.info("Deleting " + inputFileName);
      Path inPath = new Path(inputFileName.toString());
      try
      {
        deleteWithRetries(inPath, false);
      }
      catch (IOException e)
      {
        LOG.error("Failed to delete file " + inputFileName + ". Skipping.", e);
      }
    }
  }
  
  private void mergeAndCopyFiles()
    throws IOException
  {
    int retriesRemaining = reducer.getNumTransferRetries();
    try
    {
      for (;;)
      {
        OutputStream outputStream = reducer.decorateOutputStream(reducer.openOutputStream(outputFile), outputFile);Throwable localThrowable3 = null;
        try
        {
          LOG.info("Opening output file: " + outputFile);
          for (FileInfo inputFile : inputFiles)
          {
            Path inputFilePath = new Path(inputFileName.toString());
            try
            {
              InputStream inputStream = reducer.decorateInputStream(reducer.openInputStream(inputFilePath), inputFilePath);Throwable localThrowable4 = null;
              try
              {
                LOG.info("Starting download of " + inputFileName + " to " + outputFile);
                long bytesCopied = copyStream(inputStream, outputStream);
                LOG.info("Copied " + bytesCopied + " bytes");
              }
              catch (Throwable localThrowable1)
              {
                localThrowable4 = localThrowable1;throw localThrowable1;
              }
              finally {}
            }
            catch (Exception e)
            {
              handlePartialOutput(outputFile, outputStream);
              throw e;
            }
            LOG.info("Finished downloading " + inputFileName);
          }
          LOG.info("Finished downloading " + inputFiles.size() + " input file(s) to " + outputFile); return;
        }
        catch (Throwable localThrowable2)
        {
          localThrowable3 = localThrowable2;throw localThrowable2;
        }
        finally
        {
          if (outputStream != null) {
            if (localThrowable3 != null) {
              try
              {
                outputStream.close();
              }
              catch (Throwable x2)
              {
                localThrowable3.addSuppressed(x2);
              }
            } else {
              outputStream.close();
            }
          }
        }
      }
    }
    catch (IOException e)
    {
      retriesRemaining--;
      LOG.warn("Exception raised while copying file data to file=" + outputFile + " numRetriesRemaining=" + retriesRemaining, e);
      if (retriesRemaining <= 0) {
        throw e;
      }
    }
  }
  
  private void handlePartialOutput(Path outputFile, OutputStream outputStream)
  {
    if (outputStream != null) {
      if ((outputStream instanceof Abortable))
      {
        LOG.warn("Output stream is abortable, aborting the output stream for " + outputFile);
        Abortable abortable = (Abortable)outputStream;
        try
        {
          abortable.abort();
          outputStream.close();
        }
        catch (IOException e)
        {
          LOG.warn("Failed to abort the output stream for " + outputFile);
        }
      }
      else
      {
        try
        {
          outputStream.close();
        }
        catch (IOException e)
        {
          LOG.warn("Failed to close the output stream for " + outputFile);
        }
        LOG.warn("Output stream is not abortable, attempting to delete partial output written to " + outputFile);
        try
        {
          deleteWithRetries(outputFile, false);
        }
        catch (Exception e2)
        {
          LOG.warn("Failed to delete partial output stored in " + outputFile);
        }
      }
    }
  }
}

/* Location:
 * Qualified Name:     com.amazon.elasticmapreduce.s3distcp.CopyFilesRunnable
 * Java Class Version: 7 (51.0)
 * JD-Core Version:    0.7.1
 */