package com.github.gbraccialli.hive.hooks;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.Serializable;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.AbstractSemanticAnalyzerHook;
import org.apache.hadoop.hive.ql.parse.HiveSemanticAnalyzerHook;
import org.apache.hadoop.hive.ql.parse.HiveSemanticAnalyzerHookContext;
import org.apache.hadoop.hive.ql.parse.SemanticException;

public class UserGroupQueueHook extends AbstractSemanticAnalyzerHook {

	private static final String MR_QUEUE_NAME_PROPERTY = "mapred.job.queue.name";
	private static final String TEZ_QUEUE_NAME_PROPERTY = "tez.queue.name";
	private static final String HIVE_EXECUTION_ENGINE_PROPERTY = "hive.execution.engine";

	private static final Log LOG = LogFactory.getLog(QueueHandlerHiveDriverRunHook.class);
	
	@Override
	public void postAnalyze(HiveSemanticAnalyzerHookContext arg0,
			List<Task<? extends Serializable>> arg1) throws SemanticException {
	}

	@Override
	public ASTNode preAnalyze(HiveSemanticAnalyzerHookContext context, ASTNode node)
			throws SemanticException {
		
		//System.out.println("semantic=" + context.getUserName());
		
		HiveConf config = (HiveConf) context.getConf();
		
		//check if hive execution engine is set to tez. If so, queue name property should be tez.queue.name.
		String queue_property = MR_QUEUE_NAME_PROPERTY;
		String hiveExecEngine = config.get(HIVE_EXECUTION_ENGINE_PROPERTY);

		if(hiveExecEngine!=null && hiveExecEngine.equalsIgnoreCase("tez")){
			queue_property = TEZ_QUEUE_NAME_PROPERTY;
		}
		
		String user = context.getUserName();
		if (user == null){
			//when using beeline/jdbc client, context.getUserName() has the username
			//when using hive cli, context.getUserName() comes null and config.getUser() has the username  
			try{
				user = config.getUser();
			}catch (Exception e){
				//do nothing
				LOG.error("Error to get username" + e.toString());
			}
		}
		String group = getDefaultUserGroupFromOS(user);
		
		LOG.info("UserGroupQueueHook found username as '" + user + "' and default group from OS as '" + group + "'");
		
		if (group.length() > 0){
			config.set(queue_property, group);
		}
		
		return node;
		
	}

	private static String getDefaultUserGroupFromOS(String user){
		Process p;
		try {
			p = Runtime.getRuntime().exec("groups " + user);

			BufferedReader stdInput = new BufferedReader(new
					InputStreamReader(p.getInputStream()));

			String groups = stdInput.readLine();

			String groupsSplit[] = groups.split(":");

			if (groupsSplit.length == 2){
				String defaultGroup = groupsSplit[1].trim().split(" ")[0].trim();
				return defaultGroup;
			}else{
				LOG.error("No groups found for user '" + user + "'");
				return "";
			}
		} catch (Exception e) {
			LOG.error("Error trying to groups for user '" + user + "':" + e.toString());
			return "";
		}
	}

}
