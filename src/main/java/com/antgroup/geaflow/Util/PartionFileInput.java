package com.antgroup.geaflow.Util;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

import static java.lang.Math.min;

public class PartionFileInput {
    static final int LINEWIDTH=1000;
    long pos;
    final long end;
    final RandomAccessFile ras;
    final ByteBuffer buffer;
    public PartionFileInput(String filename, int parallel, int index){
        try {
            File file = new File(filename);
            ras = new RandomAccessFile(file, "r");
            long size=file.length();
            pos= size*index/parallel;
            end= size*(index+1)/parallel;
            buffer = ras.getChannel().map(FileChannel.MapMode.READ_ONLY, pos, min(LINEWIDTH+end,size)-pos).asReadOnlyBuffer();
        } catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Move to next Line
     */
    public boolean nextLine() throws IOException {
        while(pos<end) {
            char ch= (char) buffer.get();
            pos++;
            if(ch=='\n')break;
        }
        return pos<end;
    }

    public Long nextLong() throws IOException {
        StringBuilder sb=new StringBuilder();
        while(true){
            char ch= (char) buffer.get();
            pos++;
            if(ch=='|'||ch=='.')break;
            sb.append(ch);
        }
        return Long.parseLong(sb.toString());
    }

    public Double nextDouble() throws IOException {
        StringBuilder sb = new StringBuilder();
        while (true) {
            char ch = (char) buffer.get();
            pos++;
            if (ch == '|') break;
            sb.append(ch);
        }
        return Double.parseDouble(sb.toString());
    }

}
