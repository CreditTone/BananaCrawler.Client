package banana.client;

import java.io.File;
import java.net.InetSocketAddress;
import java.util.Scanner;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;

import com.alibaba.fastjson.JSON;

import banana.core.modle.CommandResponse;
import banana.core.modle.Task;
import banana.core.modle.TaskStatus;
import banana.core.modle.TaskStatus.DownloaderTrackerStatus;
import banana.core.protocol.MasterProtocol;

public class Command {

	public static void main(String[] args) throws Exception {
		args = (args == null || args.length == 0) ? new String[] {"-h"} : args;
		CommandLineParser parser = new DefaultParser();
		Options options = new Options();
		options.addOption("h", "help", false, "print this usage information");
		options.addOption("c", "config", true, "config file path");
		options.addOption("s", "submit", true, "submit task from a jsonfile");
		options.addOption("st", "stoptask", true, "stop a task");
		options.addOption("ct", "continue", true, "continue a task");
		options.addOption("sc", "stopcluster", false, "stop all task and cluster");
		CommandLine commandLine = parser.parse(options, args);
		HelpFormatter formatter = new HelpFormatter();
		if (commandLine.hasOption('h')) {
			formatter.printHelp("bananacrawler", options);
			return;
		}
		File configFile = new File("client_config.json");
		if (commandLine.hasOption("c")) {
			configFile = new File(commandLine.getOptionValue("c"));
		} else if (!configFile.exists()) {
			try {
				configFile = new File(Command.class.getClassLoader().getResource("").getPath() + "/client_config.json");
			} catch (Exception e) {
				System.out.println("请指定配置文件位置");
				System.exit(0);
			}
		}
		ClientConfig config = JSON.parseObject(FileUtils.readFileToString(configFile), ClientConfig.class);
		MasterProtocol remoteMaster = (MasterProtocol) RPC.getProxy(MasterProtocol.class, MasterProtocol.versionID,
				new InetSocketAddress(config.master.host, config.master.port), new Configuration());
		if (commandLine.hasOption("st")) {
			String taskid = commandLine.getOptionValue("st");
			CommandResponse response = remoteMaster.stopTaskById(taskid);
			if (response.success) {
				System.out.println(taskid + " stoped");
			} else {
				System.out.println(taskid + " not existd");
			}
			return;
		}
		if (commandLine.hasOption("sc")) {
			try {
				remoteMaster.stopCluster();
			} catch (Exception e) {}
			System.out.println("cluster stoped");
			return;
		}
		Scanner scan = new Scanner(System.in);
		// submit是提交任务
		if (commandLine.hasOption('s') || commandLine.hasOption("continue")) {
			Task task = null;
			if (commandLine.hasOption('s')){
				String taskFilePath = commandLine.getOptionValue('s');
				task = JSON.parseObject(FileUtils.readFileToString(new File(taskFilePath), "UTF-8"), Task.class);
			}else{
				String taskFilePath = commandLine.getOptionValue("ct");
				System.out.println(taskFilePath);
				task = JSON.parseObject(FileUtils.readFileToString(new File(taskFilePath), "UTF-8"), Task.class);
				if (task.allow_multi_task){
					throw new Exception("多任务模式不允许共享任务状态");
				}
				task.synchronizeLinks = true;
			}
			task.verify();
			System.out.println("submit......");
			CommandResponse response = remoteMaster.submitTask(task);
			if (response.success){
				if (response.msg.length() > 0){
					TaskStatus status = remoteMaster.getTaskStatusById(response.msg);
					System.out.println("taskname:" + status.name);
					System.out.println("id:" + status.id);
					System.out.println("filer:" + task.filter);
					System.out.println("queue suport_priority:" + (task.queue.get("suport_priority")!=null?task.queue.get("suport_priority"):false) + " delay:"+(task.queue.get("delay")!=null?task.queue.get("delay"):0));
					System.out.println("processor count:" + task.processors.size());
					if (task.mode != null){
						System.out.println("mode:" + JSON.toJSONString(task.mode));
					}
					System.out.println("stat:" + status.stat.toString());
					for (DownloaderTrackerStatus dts : status.downloaderTrackerStatus) {
						System.out.println("=========" + "downloader tracker" + "=========");
						System.out.println("owner:" + dts.owner);
						System.out.println("thread:" + dts.thread);
						System.out.println("stat:" + dts.stat);
					}
				}else{
					if (task.mode != null){
						System.out.println("queue suport_priority:" + (task.queue.get("suport_priority")!=null?task.queue.get("suport_priority"):false) + " delay:"+(task.queue.get("delay")!=null?task.queue.get("delay"):0));
						System.out.println("filer:" + task.filter);
						if (task.mode != null){
							System.out.println("mode:" + JSON.toJSONString(task.mode));
						}
					}
				}
			}else{
				System.out.println("error:"+response.msg);
			}
		}
	}

}
