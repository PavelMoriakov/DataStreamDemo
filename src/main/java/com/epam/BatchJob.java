/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.epam;

import com.epam.model.Speaker;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.MapOperator;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.util.Collector;

import java.util.List;

/**
 * Skeleton for a Flink Batch Job.
 *
 * <p>For a tutorial how to write a Flink batch application, check the
 * tutorials and examples on the <a href="https://flink.apache.org/docs/stable/">Flink Website</a>.
 *
 * <p>To package your application into a JAR file for execution,
 * change the main class in the POM.xml file to this class (simply search for 'mainClass')
 * and run 'mvn clean package' on the command line.
 */
public class BatchJob {

	public static void main(String[] args) throws Exception {
		// set up the batch execution environment


		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		env.setParallelism(1);


		DataSource<Tuple5<String, String, String, String, String>> types = env.readCsvFile("data/oscar_age_male" +
				".csv")
				.ignoreFirstLine()
				.includeFields("11111")
				.ignoreInvalidLines()
				.types(String.class, String.class, String.class, String.class, String.class);

		//types.print();

		types.map(new MapFunction<Tuple5<String, String, String, String, String>, Tuple2<String,Integer>>() {
			@Override
			public Tuple2<String, Integer> map(Tuple5<String, String, String, String, String> value) throws Exception {
				return Tuple2.of(value.f2, 1);
			}
		}).groupBy(0)
				.sum(1).first(25)
				.sortPartition(1, Order.DESCENDING)
				.print();


		env.execute("Flink Batch Java API Skeleton");
	}

}
