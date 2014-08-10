package com.springsource.open.db;

import java.util.Date;

import javax.sql.DataSource;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.simple.SimpleJdbcTemplate;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

@Repository
public class SimpleAuditService implements AuditService {

	private SimpleJdbcTemplate jdbcTemplate;

	@Autowired
	public void setDataSource(@Qualifier("otherDataSource") DataSource dataSource) {
		this.jdbcTemplate = new SimpleJdbcTemplate(dataSource);
	}

	@Transactional(propagation=Propagation.REQUIRES_NEW)
	public void update(int id, String operation, String name) {
		jdbcTemplate.update(
				"INSERT into T_AUDITS (id,operation,name,audit_date) "
						+ "values (?,?,?,?)", id, operation, name, new Date());
	}

}
