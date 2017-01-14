package com.fs.spark.solr.writer;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.PosixParser;
import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.FileAppender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;
import org.apache.solr.common.SolrInputDocument;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.sql.types.StructType;

import com.lucidworks.spark.util.SolrSupport;

import java.io.File;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

/**
 * Main class to load data from hdfs to solr collection
 * before run this collection should be created in solr as per schema of the input data
 * 
 * @author hayatb
 */
public class FreestoneSolrWriter implements Serializable {

	private static Logger logger = Logger.getLogger(FreestoneSolrWriter.class);

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	protected static transient SparkSession sparkSession;

	private static boolean consoleLog = false;

	public FreestoneSolrWriter(String filePath, String level, String applicationName) throws Exception {
		initLogger(filePath, level, applicationName);
	}

	 public enum CmdLineArg {
		input_hdfs_path, format, zkhost_str, solr_collection, batch_size, log_file_path, application_name, spark_master, log_level
	}

	@SuppressWarnings("static-access")
	public static void main(String[] args) {
		try {
			CommandLineParser parser = new PosixParser();
			Options options = new Options();
			options.addOption(OptionBuilder.withLongOpt(CmdLineArg.input_hdfs_path.name()).withDescription("Hdfs file path")
					.isRequired(true).withType(String.class).hasArg().create(CmdLineArg.input_hdfs_path.name()));
			options.addOption(
					OptionBuilder.withLongOpt(CmdLineArg.format.name()).withDescription("input file format (json,orc etc)")
							.isRequired(true).withType(String.class).hasArg().create());
			options.addOption(OptionBuilder.withLongOpt(CmdLineArg.zkhost_str.name()).withDescription("Solr zkhost str").isRequired(true)
					.withType(String.class).hasArg().create());
			options.addOption(OptionBuilder.withLongOpt(CmdLineArg.solr_collection.name()).withDescription("solr collection name")
					.isRequired(true).withType(String.class).hasArg().create());
			options.addOption(OptionBuilder.withLongOpt(CmdLineArg.batch_size.name()).withDescription("batch size").isRequired(true)
					.withType(Integer.class).hasArg().create());
			options.addOption(OptionBuilder.withLongOpt(CmdLineArg.log_file_path.name()).withDescription("Log file path").isRequired(true)
					.withType(String.class).hasArg().create());
			options.addOption(OptionBuilder.withLongOpt(CmdLineArg.application_name.name()).withDescription("Application name")
					.isRequired(true).withType(String.class).hasArg().create());
			options.addOption(OptionBuilder.withLongOpt(CmdLineArg.spark_master.name())
					.withDescription("Spark master (yarn-client,local,local[*])").isRequired(true).withType(String.class).hasArg()
					.create());
			options.addOption(OptionBuilder.withLongOpt(CmdLineArg.log_level.name()).withDescription("Log level(INFO,DEBUG etc)")
					.isRequired(true).withType(String.class).hasArg().create());

			// parse cmd line arguments
			CommandLine line = parser.parse(options, args, true);
			String hdfsPath = line.getOptionValue(CmdLineArg.input_hdfs_path.name());
			String logLevel = line.getOptionValue(CmdLineArg.log_level.name());
			String logFilePath = line.getOptionValue(CmdLineArg.log_file_path.name());
			String format = line.getOptionValue(CmdLineArg.format.name());
			String zkHost = line.getOptionValue(CmdLineArg.zkhost_str.name());
			String collection = line.getOptionValue(CmdLineArg.solr_collection.name());
			int batchSize = Integer.parseInt(line.getOptionValue(CmdLineArg.batch_size.name()));
			String applicationName = line.getOptionValue(CmdLineArg.application_name.name());
			String sparkMaster = line.getOptionValue(CmdLineArg.spark_master.name());
			SparkConf conf = new SparkConf().setAppName(applicationName).setMaster(sparkMaster);
			sparkSession = SparkSession.builder().config(conf).getOrCreate();
			int commitWithin = 10000;
			FreestoneSolrWriter sorlWriter = new FreestoneSolrWriter(logFilePath, logLevel, applicationName);
			sorlWriter.loadIntoSolr(zkHost, collection, hdfsPath, batchSize, format, sparkSession, commitWithin);
		} catch (Exception e) {
			logger.error("Got exception in main method", e);
			System.err.println();
		} finally {
			if (sparkSession != null) {
				sparkSession.stop();
			}
		}
	}

	private void loadIntoSolr(String zkHost, String collection, String hdfsPath, int batchSize, String format,
			SparkSession sparkSession, int commitWithin) {
		logger.debug("Loading hdfs file path= "+hdfsPath +" format ="+format);
		Dataset<Row> dataset = sparkSession.read().format(format).load(hdfsPath);
		logger.debug("start transformationFuncation to convert Rdd<Row> to Rdd<SolrInputDocument>");
		Function<Row, SolrInputDocument> transformationFuncation = new Function<Row, SolrInputDocument>() {
			private static final long serialVersionUID = 1L;

			public SolrInputDocument call(Row arg0) throws Exception {
				return rowToSolrDocument(arg0);
			}
		};
		JavaRDD<SolrInputDocument> docs = dataset.javaRDD().map(transformationFuncation);
		logger.debug("end transformationFuncation to convert Rdd<Row> to Rdd<SolrInputDocument> solrRddCount="+docs.count());
		Object object = new Integer(commitWithin);
		scala.Option<Object> commitWithinOption = scala.Option.apply(object);
		JavaRDD<SolrInputDocument> updatedDocs = docs.repartition(16);
		SolrSupport.indexDocs(zkHost, collection, batchSize, updatedDocs.rdd(), commitWithinOption);
	}

	private static void rowToHasMap(GenericRowWithSchema rowWithSchema, Map<String, Object> records) {
		StructType structType = rowWithSchema.schema();
		Object[] objects = rowWithSchema.values();

		String fieldNames[] = structType.fieldNames();
		int index = 0;
		for (Object cellValue : objects) {
			String name = fieldNames[index];
			if (cellValue instanceof GenericRowWithSchema) {
				GenericRowWithSchema innerRow = (GenericRowWithSchema) cellValue;
				rowToHasMap(innerRow, records);
			} else {
				if (cellValue != null) {
					records.put(name, cellValue.toString());
				}
			}
			index++;
		}
	}

	private static SolrInputDocument rowToSolrDocument(Row row) throws Exception {
		SolrInputDocument doc = null;
		if (row instanceof GenericRowWithSchema) {
			Map<String, Object> record = new HashMap<String, Object>();
			GenericRowWithSchema genericRowWithSchema = (GenericRowWithSchema) row;
			rowToHasMap(genericRowWithSchema, record);
			if (record != null && !record.isEmpty()) {
				doc = new SolrInputDocument();
				for (Entry<String, Object> entry : record.entrySet()) {
					String key = entry.getKey();
					Object value = entry.getValue();
					doc.addField(key, value.toString());
				}
			}
		}
		return doc;
	}

	private void initLogger(String filePath, String level, String applicationName) throws Exception {
		File file = new File(filePath);
		File parentFile = file.getParentFile();
		if (parentFile != null && !parentFile.exists()) {
			boolean isParentCreated = parentFile.mkdirs();
			if (!isParentCreated) {
				System.err.println("Failed to create log dir: " + parentFile.getAbsolutePath());
				throw new Exception("Failed to create log dir: " + parentFile.getAbsolutePath());
			}
		}
		Logger logger = Logger.getLogger("com.fs.spark.writer");
		logger.addAppender(createFileAppender(applicationName, filePath, level));
		logger.setLevel(Level.toLevel(level));
		logger.setAdditivity(true);
		// root logger to log spark application logs
		Logger rootLogger = Logger.getRootLogger();
		rootLogger.addAppender(createFileAppender(applicationName + "_root", filePath + "root", level));
		rootLogger.setLevel(Level.ERROR);
		rootLogger.setAdditivity(true);

		if (consoleLog) {
			rootLogger.addAppender(createConsoleLogAppender(applicationName + "_console", level));
		}
	}

	private FileAppender createFileAppender(String name, String filePath, String level) {
		FileAppender fa = new FileAppender();
		fa.setName(name);
		fa.setFile(filePath);
		fa.setLayout(new PatternLayout("%d [%t] %-5p %C{6} (%F:%L) - %m%n"));
		fa.setThreshold(Level.toLevel(level));
		fa.setAppend(true);
		fa.activateOptions();
		return fa;

	}

	private ConsoleAppender createConsoleLogAppender(String name, String level) {
		ConsoleAppender consoleAppender = new ConsoleAppender();
		consoleAppender.setName(name);
		consoleAppender.setLayout(new PatternLayout("%d [%t] %-5p %C{6} (%F:%L) - %m%n"));
		consoleAppender.setThreshold(Level.toLevel(level));
		return consoleAppender;

	}
}