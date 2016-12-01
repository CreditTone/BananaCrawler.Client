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
import banana.core.modle.TaskStatus;
import banana.core.modle.TaskStatus.DownloaderTrackerStatus;
import banana.core.protocol.MasterProtocol;
import banana.core.protocol.Task;

public class Command {

	public static void main(String[] args) throws Exception {
		args = (args == null || args.length == 0) ? new String[] {"-h"} : args;
		CommandLineParser parser = new DefaultParser();
		Options options = new Options();
		options.addOption("h", "help", false, "print this usage information");
		options.addOption("c", "config", true, "config file path");
		options.addOption("s", "submit", true, "submit task from a jsonfile");
		options.addOption("st", "stoptask", true, "stop a task");
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
			String taskname = commandLine.getOptionValue("st");
			CommandResponse response = remoteMaster.stopTask(taskname);
			if (response.success) {
				System.out.println(taskname + " stoped");
			} else {
				System.out.println(taskname + " not existd");
			}
			return;
		}
		if (commandLine.hasOption("sc")) {
			try {
				remoteMaster.stopCluster();
			} catch (Exception e) {
			}
			System.out.println("cluster stoped");
			return;
		}
		Scanner scan = new Scanner(System.in);
		// submit是提交任务
		if (commandLine.hasOption('s')) {
			String taskFilePath = commandLine.getOptionValue('s');
			Task task = JSON.parseObject(FileUtils.readFileToString(new File(taskFilePath), "UTF-8"), Task.class);
			task.verify();
			TaskStatus taskStatus = remoteMaster.taskStatus(task);
			if (taskStatus.stat == TaskStatus.Stat.Runing) {
				System.out.print("任务" + task.name + "已经在运行，是否对" + task.name + "任务配置进行更新?\n确认请输入 y/yes:");
				String yes = scan.next();
				if (!yes.equalsIgnoreCase("Y") && !yes.equalsIgnoreCase("YES")) {
					System.out.println("Task to submit cancel.");
					return;
				}
			}
			if (taskStatus.dataCount > 0) {
				System.out.print("集合" + task.collection + "下，taskname为" + task.name + "已经有" + taskStatus.dataCount + "条数据,");
				System.out.print("是否要删除?\n确认请输入 y/yes:");
				String yes = scan.next();
				if (yes.equalsIgnoreCase("Y") || yes.equalsIgnoreCase("YES")) {
					String password = null;
					for (int i = 0; i < 3; i++) {
						System.out.print("password:");
						password = scan.next();
						if (remoteMaster.verifyPassword(password).get()) {
							int n = remoteMaster.removeBeforeResult(task.collection, task.name).get();
							System.out.println("Delete article " + n);
							break;
						} else {
							System.out.println("password error.");
							if (i == 3) {
								return;
							}
						}
					}
				}
			}
			if (taskStatus.requestCount > 0){
				System.out.print("系统检测到上次还有一些requests是否加载上次剩余的requests?\n确认请输入 y/yes:");
				String yes = scan.next();
				if (yes.equalsIgnoreCase("Y") || yes.equalsIgnoreCase("YES")) {
					task.synchronizeLinks = true;
				}
			}
			CommandResponse response = remoteMaster.submitTask(task);
			if (response.success){
				TaskStatus status = remoteMaster.taskStatus(task);
				System.out.println("task infos:");
				System.out.println("taskname:" + status.name);
				System.out.println("id:" + status.id);
				System.out.println("stat:" + status.stat.toString());
				if (!status.downloaderTrackerStatus.isEmpty()){
					System.out.println("downloader tracker infos:");
					for (DownloaderTrackerStatus dts : status.downloaderTrackerStatus) {
						System.out.println("=========" + "downloader tracker" + "=========");
						System.out.println("owner:" + dts.owner);
						System.out.println("thread:" + dts.thread);
						System.out.println("stat:" + dts.stat);
					}
				}
			}else{
				System.out.println(response.error);
			}
		}
	}

}
