package com.alibaba.jstorm.yarn.utils;

import ch.ethz.ssh2.Connection;
import ch.ethz.ssh2.Session;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;


/**
 * Created by fengjian on 16/1/6.
 */
public class MyScpClient {


    Connection conn;

    class LenNamePair {
        long length;
        String filename;
    }

    public MyScpClient(Connection conn) {
        if (conn == null)
            throw new IllegalArgumentException("Cannot accept null argument!");
        this.conn = conn;
    }

    private void readResponse(InputStream is) throws IOException {
        int c = is.read();

        if (c == 0)
            return;

        if (c == -1)
            throw new IOException("Remote scp terminated unexpectedly.");

        if ((c != 1) && (c != 2))
            throw new IOException("Remote scp sent illegal error code.");

        if (c == 2)
            throw new IOException("Remote scp terminated with error.");

        String err = receiveLine(is);
        throw new IOException("Remote scp terminated with error (" + err + ").");
    }

    private String receiveLine(InputStream is) throws IOException {
        StringBuffer sb = new StringBuffer(30);

        while (true) {
            /* This is a random limit - if your path names are longer, then adjust it */

            if (sb.length() > 8192)
                throw new IOException("Remote scp sent a too long line");

            int c = is.read();

            if (c < 0)
                throw new IOException("Remote scp terminated unexpectedly.");

            if (c == '\n')
                break;

            sb.append((char) c);

        }
        return sb.toString();
    }

    private LenNamePair parseCLine(String line) throws IOException {
		/* Minimum line: "xxxx y z" ---> 8 chars */

        long len;

        if (line.length() < 8)
            throw new IOException("Malformed C line sent by remote SCP binary, line too short.");

        if ((line.charAt(4) != ' ') || (line.charAt(5) == ' '))
            throw new IOException("Malformed C line sent by remote SCP binary.");

        int length_name_sep = line.indexOf(' ', 5);

        if (length_name_sep == -1)
            throw new IOException("Malformed C line sent by remote SCP binary.");

        String length_substring = line.substring(5, length_name_sep);
        String name_substring = line.substring(length_name_sep + 1);

        if ((length_substring.length() <= 0) || (name_substring.length() <= 0))
            throw new IOException("Malformed C line sent by remote SCP binary.");

        if ((6 + length_substring.length() + name_substring.length()) != line.length())
            throw new IOException("Malformed C line sent by remote SCP binary.");

        try {
            len = Long.parseLong(length_substring);
        } catch (NumberFormatException e) {
            throw new IOException("Malformed C line sent by remote SCP binary, cannot parse file length.");
        }

        if (len < 0)
            throw new IOException("Malformed C line sent by remote SCP binary, illegal file length.");

        LenNamePair lnp = new LenNamePair();
        lnp.length = len;
        lnp.filename = name_substring;

        return lnp;
    }

    private void sendBytes(Session sess, byte[] data, String fileName, String mode) throws IOException {
        OutputStream os = sess.getStdin();
        InputStream is = new BufferedInputStream(sess.getStdout(), 512);

        readResponse(is);

        String cline = "C" + mode + " " + data.length + " " + fileName + "\n";

        os.write(cline.getBytes());
        os.flush();

        readResponse(is);

        os.write(data, 0, data.length);
        os.write(0);
        os.flush();

        readResponse(is);

        os.write("E\n".getBytes());
        os.flush();
    }

    private void sendFiles(Session sess, String[] files, String mode) throws IOException {
        byte[] buffer = new byte[8192];

        OutputStream os = new BufferedOutputStream(sess.getStdin(), 40000);
        InputStream is = new BufferedInputStream(sess.getStdout(), 512);

        readResponse(is);

        for (int i = 0; i < files.length; i++) {
            File f = new File(files[i]);
            long remain = f.length();

            String cline = "C" + mode + " " + remain + " " + f.getName() + "\n";

            os.write(cline.getBytes());
            os.flush();

            readResponse(is);

            FileInputStream fis = null;

            try {
                fis = new FileInputStream(f);

                while (remain > 0) {
                    int trans;
                    if (remain > buffer.length)
                        trans = buffer.length;
                    else
                        trans = (int) remain;

                    if (fis.read(buffer, 0, trans) != trans)
                        throw new IOException("Cannot read enough from local file " + files[i]);

                    os.write(buffer, 0, trans);

                    remain -= trans;
                }

                fis.close();
            } catch (IOException e) {
                if (fis != null) {
                    fis.close();
                }
                throw (e);
            }

            os.write(0);
            os.flush();

            readResponse(is);
        }

        os.write("E\n".getBytes());
        os.flush();
    }

    private void receiveFiles(Session sess, String[] files, String target) throws IOException {
        byte[] buffer = new byte[8192];

        OutputStream os = new BufferedOutputStream(sess.getStdin(), 512);
        InputStream is = new BufferedInputStream(sess.getStdout(), 40000);

        os.write(0x0);
        os.flush();

        for (int i = 0; i < files.length; i++) {
            LenNamePair lnp = null;

            while (true) {
                int c = is.read();
                if (c < 0)
                    throw new IOException("Remote scp terminated unexpectedly.");

                String line = receiveLine(is);

                if (c == 'P') {
					/* Ignore modification times */

                    continue;
                }

                if ((c == 1) || (c == 2))
                    throw new IOException("Remote SCP error: " + line);

                if (c == 'C') {
                    lnp = parseCLine(line);
                    break;

                }
                throw new IOException("Remote SCP error: " + ((char) c) + line);
            }

            os.write(0x0);
            os.flush();

            File f = new File(target + File.separatorChar + lnp.filename);
            FileOutputStream fop = null;

            try {
                fop = new FileOutputStream(f);

                long remain = lnp.length;

                while (remain > 0) {
                    int trans;
                    if (remain > buffer.length)
                        trans = buffer.length;
                    else
                        trans = (int) remain;

                    int this_time_received = is.read(buffer, 0, trans);

                    if (this_time_received < 0) {
                        throw new IOException("Remote scp terminated connection unexpectedly");
                    }

                    fop.write(buffer, 0, this_time_received);

                    remain -= this_time_received;
                }

                fop.close();
            } catch (IOException e) {
                if (fop != null)
                    fop.close();

                throw (e);
            }

            readResponse(is);

            os.write(0x0);
            os.flush();
        }
    }

    /**
     * Copy a local file to a remote directory, uses mode 0600 when creating
     * the file on the remote side.
     *
     * @param localFile             Path and name of local file.
     * @param remoteTargetDirectory Remote target directory.
     * @throws IOException
     */
    public void put(String localFile, String remoteTargetDirectory) throws IOException {
        put(new String[]{localFile}, remoteTargetDirectory, "0600");
    }

    /**
     * Copy a set of local files to a remote directory, uses mode 0600 when
     * creating files on the remote side.
     *
     * @param localFiles            Paths and names of local file names.
     * @param remoteTargetDirectory Remote target directory.
     * @throws IOException
     */

    public void put(String[] localFiles, String remoteTargetDirectory) throws IOException {
        put(localFiles, remoteTargetDirectory, "0600");
    }

    /**
     * Copy a local file to a remote directory, uses the specified mode when
     * creating the file on the remote side.
     *
     * @param localFile             Path and name of local file.
     * @param remoteTargetDirectory Remote target directory.
     * @param mode                  a four digit string (e.g., 0644, see "man chmod", "man open")
     * @throws IOException
     */
    public void put(String localFile, String remoteTargetDirectory, String mode) throws IOException {
        put(new String[]{localFile}, remoteTargetDirectory, mode);
    }

    /**
     * Create a remote file and copy the contents of the passed byte array into it.
     * Uses mode 0600 for creating the remote file.
     *
     * @param data                  the data to be copied into the remote file.
     * @param remoteFileName        The name of the file which will be created in the remote target directory.
     * @param remoteTargetDirectory Remote target directory.
     * @throws IOException
     */

    public void put(byte[] data, String remoteFileName, String remoteTargetDirectory) throws IOException {
        put(data, remoteFileName, remoteTargetDirectory, "0600");
    }

    /**
     * Create a remote file and copy the contents of the passed byte array into it.
     * The method use the specified mode when creating the file on the remote side.
     *
     * @param data                  the data to be copied into the remote file.
     * @param remoteFileName        The name of the file which will be created in the remote target directory.
     * @param remoteTargetDirectory Remote target directory.
     * @param mode                  a four digit string (e.g., 0644, see "man chmod", "man open")
     * @throws IOException
     */
    public void put(byte[] data, String remoteFileName, String remoteTargetDirectory, String mode) throws IOException {
        Session sess = null;

        if ((remoteFileName == null) || (remoteTargetDirectory == null) || (mode == null))
            throw new IllegalArgumentException("Null argument.");

        if (mode.length() != 4)
            throw new IllegalArgumentException("Invalid mode.");

        for (int i = 0; i < mode.length(); i++)
            if (Character.isDigit(mode.charAt(i)) == false)
                throw new IllegalArgumentException("Invalid mode.");

        String cmd = "scp -t -d " + remoteTargetDirectory;

        try {
            sess = conn.openSession();
            sess.execCommand(cmd);
            sendBytes(sess, data, remoteFileName, mode);
            sess.close();
        } catch (IOException e) {
            if (sess != null)
                sess.close();
            throw (IOException) new IOException("Error during SCP transfer.").initCause(e);
        }
    }

    /**
     * Copy a set of local files to a remote directory, uses the specified mode
     * when creating the files on the remote side.
     *
     * @param localFiles            Paths and names of the local files.
     * @param remoteTargetDirectory Remote target directory.
     * @param mode                  a four digit string (e.g., 0644, see "man chmod", "man open")
     * @throws IOException
     */
    public void put(String[] localFiles, String remoteTargetDirectory, String mode) throws IOException {
        Session sess = null;

        if ((localFiles == null) || (remoteTargetDirectory == null) || (mode == null))
            throw new IllegalArgumentException("Null argument.");

        if (mode.length() != 4)
            throw new IllegalArgumentException("Invalid mode.");

        for (int i = 0; i < mode.length(); i++)
            if (Character.isDigit(mode.charAt(i)) == false)
                throw new IllegalArgumentException("Invalid mode.");

        if (localFiles.length == 0)
            return;

        String cmd = "scp -t -d " + remoteTargetDirectory;

        for (int i = 0; i < localFiles.length; i++) {
            if (localFiles[i] == null)
                throw new IllegalArgumentException("Cannot accept null filename.");
        }

        try {
            sess = conn.openSession();
            sess.execCommand(cmd);
            sendFiles(sess, localFiles, mode);
            sess.close();
        } catch (IOException e) {
            if (sess != null)
                sess.close();
            throw (IOException) new IOException("Error during SCP transfer.").initCause(e);
        }
    }

    /**
     * Download a file from the remote server to a local directory.
     *
     * @param remoteFile           Path and name of the remote file.
     * @param localTargetDirectory Local directory to put the downloaded file.
     * @throws IOException
     */
    public void get(String remoteFile, String localTargetDirectory) throws IOException {
        get(new String[]{remoteFile}, localTargetDirectory);
    }

    /**
     * Download a set of files from the remote server to a local directory.
     *
     * @param remoteFiles          Paths and names of the remote files.
     * @param localTargetDirectory Local directory to put the downloaded files.
     * @throws IOException
     */
    public void get(String remoteFiles[], String localTargetDirectory) throws IOException {
        Session sess = null;

        if ((remoteFiles == null) || (localTargetDirectory == null))
            throw new IllegalArgumentException("Null argument.");

        if (remoteFiles.length == 0)
            return;

//        String cmd = "scp -f -r";
        String cmd = "scp -f ";

        for (int i = 0; i < remoteFiles.length; i++) {
            if (remoteFiles[i] == null)
                throw new IllegalArgumentException("Cannot accept null filename.");

            cmd += (" " + remoteFiles[i]);
        }

        try {
            sess = conn.openSession();
            sess.execCommand(cmd);
            receiveFiles(sess, remoteFiles, localTargetDirectory);
            sess.close();
        } catch (IOException e) {
            if (sess != null)
                sess.close();

            throw (IOException) new IOException("Error during SCP transfer.").initCause(e);
        }
    }
}
