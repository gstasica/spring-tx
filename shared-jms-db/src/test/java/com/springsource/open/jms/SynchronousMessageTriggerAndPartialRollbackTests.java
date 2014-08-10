/*
 * Copyright 2006-2007 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.springsource.open.jms;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import javax.sql.DataSource;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.simple.SimpleJdbcTemplate;
import org.springframework.jms.connection.SynchedLocalTransactionFailedException;
import org.springframework.jms.core.JmsTemplate;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.test.jdbc.SimpleJdbcTestUtils;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.support.TransactionCallback;
import org.springframework.transaction.support.TransactionTemplate;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations = "/META-INF/spring/jms-context.xml")
public class SynchronousMessageTriggerAndPartialRollbackTests {

	@Autowired
	private JmsTemplate jmsTemplate;

	@Autowired
	private PlatformTransactionManager transactionManager;

	@Autowired
	private FailureSimulator failureSimulator;

	private SimpleJdbcTemplate jdbcTemplate;

	@Autowired
	public void setDataSource(@Qualifier("dataSource") DataSource dataSource) {
		this.jdbcTemplate = new SimpleJdbcTemplate(dataSource);
	}

	@Before
	public void clearData() throws Exception {

		getMessages(); // drain queue
		jmsTemplate.convertAndSend("queue", "foo");
		jmsTemplate.convertAndSend("queue", "bar");
		jdbcTemplate.update("delete from T_FOOS");
	}

	/**
	 * We expect a total rollback, despite the fact that the business processing
	 * was successful and only the JMS commit failed.
	 * 
	 * @throws Exception
	 */
	@After
	public void checkPostConditions() throws Exception {

		assertEquals(0, SimpleJdbcTestUtils.countRowsInTable(jdbcTemplate, "T_FOOS"));
		List<String> list = getMessages();
		assertEquals(2, list.size());

	}

	@Test(expected = SynchedLocalTransactionFailedException.class)
	@DirtiesContext
	public void testReceiveMessageUpdateDatabase() throws Throwable {

		/*
		 * We can't actually test this using @Transactional because the
		 * exception we expect is going to come on commit, which happens in the
		 * test framework in that case, outside this method. So we have to use a
		 * TransactionTemplate.
		 */

		new TransactionTemplate(transactionManager).execute(new TransactionCallback() {
			public Object doInTransaction(TransactionStatus status) {

				try {
					doReceiveAndInsert();
				}
				catch (Exception e) {
					throw new RuntimeException(e);
				}

				return null;

			}

		});

	}

	private void doReceiveAndInsert() throws Exception {

		List<String> list = getMessages();
		assertEquals(2, list.size());
		assertTrue(list.contains("foo"));

		int id = 0;
		for (String name : list) {
			jdbcTemplate.update("INSERT INTO T_FOOS (id,name,foo_date) values (?,?,?)", id++, name, new Date());
		}

		assertEquals(2, SimpleJdbcTestUtils.countRowsInTable(jdbcTemplate, "T_FOOS"));

		// Simulate infrastructure failure in the messaging system...
		failureSimulator.simulateMessageSystemFailure();

	}

	private List<String> getMessages() {
		String next = "";
		List<String> msgs = new ArrayList<String>();
		while (next != null) {
			next = (String) jmsTemplate.receiveAndConvert("queue");
			if (next != null)
				msgs.add(next);
		}
		return msgs;
	}
}
