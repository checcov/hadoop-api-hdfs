package com.atguigu.test;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.net.URL;

import javax.ws.rs.PUT;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.io.IOUtils;
import org.apache.logging.log4j.core.config.plugins.convert.TypeConverters.UrlConverter;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.sun.tools.classfile.InnerClasses_attribute.Info;
/**
 * 
 * @author chenjiang 常见javaAPI操作HDFS相关Demo
 *
 */
@SuppressWarnings("all")
public class HDFSClientDemo {
	
	private static final Logger logger = LoggerFactory.getLogger(HDFSClientDemo.class);
	
	public static Configuration configuration=null;
	public static FileSystem fileSystem =null;
	
	@Before
	public void befo(){
		configuration= new Configuration();
		try {
			try {
				fileSystem = FileSystem.get(URI.create("hdfs://hadoop01:9000"), configuration, "hadoop");
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		} catch (IOException e) {
			logger.info("初始化错误");
			e.printStackTrace();
		}
	}
	/**
	 * 创建HDFS目录
	 * @throws InterruptedException 
	 * @throws IOException 
	 */
	@Test
	public void mkdir() throws IOException, InterruptedException{
		
		 Boolean result=fileSystem.mkdirs(new Path("/testoutput"));
		 if(result){
			logger.info("创建成功");
		}else{
			logger.info("创建失败");
		}
		fileSystem.close();
	}
	
	/**
	 * 使用java API 测试上传
	 * **/
	@Test
	public void put(){
		try {
			fileSystem.copyFromLocalFile(new Path("c:\\1.txt"), new Path("/testoutput/2.txt"));
		} catch (IllegalArgumentException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}finally{
			if(fileSystem!=null){
				try {
					fileSystem.close();
				} catch (IOException e) {
					logger.info("关闭fileSystem流失败");
					e.printStackTrace();
				}
			}
		}
	}
	/**
	 * 获取数据到本地
	 */
	@Test
	public void get(){
		try {
			fileSystem.copyToLocalFile(false, new Path("/testoutput/2.txt"), new Path("C:\\testHdstest.txt"),true);
		} catch (IllegalArgumentException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}finally{
			if(fileSystem!=null){
				try {
					fileSystem.close();
				} catch (IOException e) {
					logger.info("关闭fileSystem流失败");
					e.printStackTrace();
				}
			}
		}
	}
	/**
	 * 文件详情
	 * */
	@Test
	public void listFilesFunctiontest(){
		try {
			RemoteIterator<LocatedFileStatus> listFiles = fileSystem.listFiles(new Path("/"), true);
			while(listFiles.hasNext()){
				LocatedFileStatus next = listFiles.next();
				logger.info("当前的目录是：{}",next.getPath().toString());
				logger.info("当前目录所属的用户组是：{}",next.getGroup());
				logger.info("当前目录的权限是：{}",next.getPermission());
				logger.info("当前目录长度：{}",next.getLen());
				BlockLocation[] blockLocations = next.getBlockLocations();
				if(blockLocations.length<0){
					logger.info("");
				}
				for (BlockLocation blockLocation : blockLocations) {
					String[] hosts = blockLocation.getHosts();
					for (String hoststr : hosts) {
						logger.info("{},目录快信息分布在：{}",next.getPath().toString(),hoststr);
					}
				}
			}
		} catch (FileNotFoundException e) {
			logger.info("没有任何文件");
			e.printStackTrace();
		} catch (IllegalArgumentException e) {
			logger.info("没有任何文件");
			e.printStackTrace();
		} catch (IOException e) {
			logger.info("读取文件失败");
			e.printStackTrace();
		}
	}
	/**
	 *测试上传文件到HDFS
	 */
	@Test
	public void putLocalFile(){
		try {
			
			long start = System.currentTimeMillis();
			
			FileInputStream fileInputStream=new FileInputStream(new File("D:\\repository.zip"));
			
			FSDataOutputStream fSDataOutputStream = fileSystem.create(new Path("/testoutput2/repository.zip"));
			
			IOUtils.copyBytes(fileInputStream, fSDataOutputStream, configuration);
			
			IOUtils.closeStream(fileInputStream);
			IOUtils.closeStream(fSDataOutputStream);
			fileSystem.close();
			System.out.println(" 客户端上传文件结束,耗时：" + (System.currentTimeMillis() - start)+"  毫秒");
		} catch (Exception e) {
			logger.info("上传失败：{}",e);
		}
	}
	/**
	 * 删除文件
	 */
	@Test
	public void delfile(){
		try {
			long start = System.currentTimeMillis();
			boolean delete = fileSystem.delete(new Path("/testoutput2/repository.zip"),true);
			if(delete){
				logger.info("删除成功!");
			}else{
				logger.info("删除失败");
			}
			System.out.println(" 客户端删除文件结束,耗时：" + (System.currentTimeMillis() - start)+"  毫秒");
		} catch (Exception e) {
		}finally {
			try {
				fileSystem.close();
			} catch (IOException e) {
				logger.info("关闭流失败：{}",e);
			}
		}
	}
}
