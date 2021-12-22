package com.training.statementservice;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import javax.sql.DataSource;

@SpringBootTest
class StatementServiceApplicationTests {

	@Autowired
	DataSource dataSource;

	@Test
	void contextLoads() {
		Assertions.assertThat(dataSource).isNotNull();
	}

}
