package com.jfshare.jdbc.dao;

import java.lang.reflect.Field;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.sql.DataSource;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.namedparam.BeanPropertySqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.jdbc.core.namedparam.SqlParameterSource;

public class GenericDao <T> {

	private JdbcTemplate jdbcTemplate;
	protected NamedParameterJdbcTemplate namedParameterJdbcTemplate;
	private Class<T> entityClass;
	private Map<String, Field> fields = new HashMap<String, Field>();;
	
	public void setDataSource(DataSource dataSource) {
		this.jdbcTemplate = new JdbcTemplate(dataSource);
        this.namedParameterJdbcTemplate = new NamedParameterJdbcTemplate(this.jdbcTemplate);
    }
	
	protected void setEntityClass(Class<T> entityClass) {
		this.entityClass = entityClass;
		Field[] efields = entityClass.getDeclaredFields();
		for(Field field : efields) {
			field.setAccessible(true);
			fields.put(field.getName(), field);
		}
	}
	
	protected JdbcTemplate getJdbcTemplate() {
		return jdbcTemplate;
	}
	
	protected NamedParameterJdbcTemplate getNamedParameterJdbcTemplate() {
		return namedParameterJdbcTemplate;
	}
	
	protected List<T> getAll(String sql, Map<String, Object> paramMap) {
	    return this.namedParameterJdbcTemplate.query(sql, paramMap, new EntityMapper());
	}
	
	protected T get(String sql, Map<String, Object> paramMap) {
	    return this.namedParameterJdbcTemplate.queryForObject(sql, paramMap, new EntityMapper());
	}
	
	protected void save(String sql, T entity) {
	    SqlParameterSource paramSource = new BeanPropertySqlParameterSource(entity);
	    getNamedParameterJdbcTemplate().update(sql, paramSource);
	}
	
	protected class EntityMapper implements RowMapper<T> {
		
		protected ResultSetMetaData rsmd = null;
		@Override
		public T mapRow(ResultSet rs, int rowNum) throws SQLException {
			ResultSetMetaData rsmd = rs.getMetaData();
			T entity = null;
			try {
				entity = entityClass.getConstructor().newInstance();
				for(int i = 1; i <= rsmd.getColumnCount(); i ++) {
					String columnLabel = rsmd.getColumnLabel(i);
					fields.get(columnLabel).set(entity, rs.getObject(columnLabel));
				}
			} catch (Exception e) {
				e.printStackTrace();
				throw new SQLException(e);
			}
			return entity;
		} 
	}
}
