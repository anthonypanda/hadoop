/**
 * 
 */
package com.pandaanthony.hdfs;

import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Reader;
import org.apache.hadoop.io.SequenceFile.Writer;
import org.apache.hadoop.io.Text;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * @author lizhuo
 *
 */
public class HdfsDemo {
	
	FileSystem fs;
	
	Configuration conf;
	
	/**
	 * ��ʼ��FileSystem��Configuration
	 * 
	 * @throws Exception
	 */
	@Before
	public void begin () throws Exception {
		conf = new Configuration ();
		fs = FileSystem.get(conf);
	}
	
	/**
	 * �ر�FileSystem
	 * 
	 */
	@After
	public void close () {
		try {
			fs.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	/**
	 * �����ļ���
	 * 
	 * @throws Exception
	 */
	@Test
	public void mkdir () throws Exception {
		System.out.println("mkdir-------------------");
		Path path = new Path ("/test");
		fs.mkdirs(path);
	}
	
	/**
	 * �����ϴ��ļ���������ļ�
	 * 
	 * @throws Exception
	 */
	@Test
	public void upload () throws Exception {
		System.out.println("upload-------------------");
		Path path = new Path ("/user/itemcf/input/(sample)sam_tianchi_2014002_rec_tmall_log.csv");
		FSDataOutputStream outputStream = fs.create(path);
		File file = new File ("D://(sample)sam_tianchi_2014002_rec_tmall_log.csv");
		FileUtils.copyFile(file, outputStream);
	}
	
	/**
	 * �鿴��Ӧ��Ŀ¼���ļ�
	 * 
	 * @throws Exception
	 */
	@Test
	public void list () throws Exception {
		System.out.println("list-------------------");
//		Path path = new Path ("/test");
		Path path = new Path ("/user/itemcf/output/step1");
		FileStatus[] fss = fs.listStatus(path);
		for (FileStatus fileStatus : fss) {
			System.out.println(fileStatus.getPath() + "----------" + fileStatus.getLen() + "----------" + fileStatus.getAccessTime());
			// ��ȡblock��Ϣ
			BlockLocation[] bls = fs.getFileBlockLocations(fileStatus, 0, fileStatus.getLen());
			for (BlockLocation bl : bls) {
				System.out.println(bl);
			}
			// ��ȡ����ĵڼ���λ�õ���Ϣ
			FSDataInputStream input = fs.open(fileStatus.getPath());
			for (int i = 0; i < 20; i ++) {
				String line = input.readLine();
				if (line != null) {
					System.out.println(line);
				}
				
			}
//			do {
//				System.out.println(input.readLine());
//			} while (input.readLine() != null);
//			input.seek(1);
//			System.out.println((char)input.readByte());
		}
		
		/*for (int j = 1; j <= 33; j ++) {
			path = new Path ("/pagerank/output/pr" + j);
			fss = fs.listStatus(path);
			for (FileStatus fileStatus : fss) {
				System.out.println(fileStatus.getPath() + "----------" + fileStatus.getLen() + "----------" + fileStatus.getAccessTime());
				// ��ȡblock��Ϣ
				BlockLocation[] bls = fs.getFileBlockLocations(fileStatus, 0, fileStatus.getLen());
				for (BlockLocation bl : bls) {
					System.out.println(bl);
				}
				// ��ȡ����ĵڼ���λ�õ���Ϣ
				FSDataInputStream input = fs.open(fileStatus.getPath());
				for (int i = 0; i < 20; i ++) {
					String line = input.readLine();
					if (line != null) {
						System.out.println(line);
					}
					
				}
			}
		}*/
		
	}
	
	/**
	 * �����ϴ��ļ��������ڴ�����С�ļ�
	 * 
	 * @throws Exception
	 */
	@Test
	public void upload2 () throws Exception {
		System.out.println("upload2-------------------");
		Path path = new Path ("/test/seq");
		Writer writer = SequenceFile.createWriter(fs, conf, path, Text.class, Text.class);
		File file = new File ("D://test");
		for (File f : file.listFiles()) {
			// ����ļ��������е��ļ�
			writer.append(new Text (f.getName()), new Text (FileUtils.readFileToString(f)));
		}
	}
	
	/**
	 * ��������С�ļ�
	 * 
	 * @throws Exception
	 */
	@Test
	public void download () throws Exception {
		System.out.println("download-------------------");
		Path path = new Path ("/test/seq");
		Reader reader = new Reader (fs, path, conf);
		Text key = new Text ();
		Text value = new Text ();
		// ������������ļ�, ��key��value����
		while (reader.next(key, value)) {
			System.out.println(key);
//			System.out.println(value);
			System.out.println("---------------------------------------");
		}
	}


}
