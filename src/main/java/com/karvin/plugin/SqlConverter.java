package com.karvin.plugin;

import com.karvin.common.SelectorHolder;
import com.karvin.common.ShardKey;
import com.karvin.rule.Rule;
import com.karvin.rule.RuleInstance;
import org.apache.oro.text.perl.Perl5Util;
import org.apache.oro.text.regex.MatchResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by yikui on 15/8/19.
 */
public class SqlConverter {

	private String seperateSpace = "[\\r\\n\\s]+";
	private String selectRegx = "/^\\s*select[\\r\\n\\s]+(\\*|[\\w_\\(\\)\\*]+([\\r\\n\\s]*,[\\r\\n\\s]*[\\w_]+)*)\\s+from[\\r\\n\\s]+([\\w_]+).*?/mxi";
	private String insertRegx = "/^\\s*insert" + seperateSpace + "into" + seperateSpace + "([\\w_]+).*?/mxi";
	private String updateRegx = "/^\\s*update" + seperateSpace + "([\\w_]+)" + seperateSpace + "set.*?/mxi";
	private String deleteRegx = "/^\\s*delete" + seperateSpace + "from" + seperateSpace + "([\\w_]+).*?/mxi";

	protected final Logger logger = LoggerFactory.getLogger(this.getClass());

	private SqlConverter() {
	}

	private static SqlConverter instance = new SqlConverter();

	public static SqlConverter getInstance() {
		return instance;
	}

	public String convertSQL(String sql, Object params, String mapperId) {
		logger.info("sql {} , params {} , mapperId {}" ,sql,params,mapperId);
		MatchResult result = null;
		String tableName   = null;
		ShardKey key = SelectorHolder.key.get();
		if(key == null){
			return sql;
		}
		Perl5Util perl5Util = new Perl5Util();
		if (perl5Util.match(selectRegx, sql)||perl5Util.match(insertRegx, sql) || perl5Util.match(updateRegx, sql) || perl5Util.match(deleteRegx, sql)) {
			try {
				result = perl5Util.getMatch();
				int groups = result.groups();
				tableName = result.group(groups - 1);
				if(isWrite(sql) && !key.isUseMaster()){
					key.setUseMaster(true);
				}
				Rule rule = RuleInstance.getInstance().match();
				if(rule == null){
					return sql;
				}
				String newTable = rule.decideTableName(key);
				int index = result.begin(groups - 1);
				StringBuilder sb = new StringBuilder();
				sb.append(sql.substring(0, index));
				sb.append(newTable + " ");
				sb.append(sql.substring(index + tableName.length()));
				return sb.toString();
			} catch (Exception e) {
				logger.error("converSQL error happen",e);
			}
		} 

		return sql;
	}

	private boolean isWrite(String sql){
		Perl5Util perl5Util = new Perl5Util();
		return (perl5Util.match(insertRegx, sql) || perl5Util.match(updateRegx, sql) || perl5Util.match(deleteRegx, sql));
	}

}
