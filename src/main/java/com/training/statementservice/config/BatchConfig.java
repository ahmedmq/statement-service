package com.training.statementservice.config;

import com.training.statementservice.domain.Transaction;
import lombok.RequiredArgsConstructor;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.database.JdbcCursorItemReader;
import org.springframework.batch.item.file.FlatFileItemWriter;
import org.springframework.batch.item.file.transform.BeanWrapperFieldExtractor;
import org.springframework.batch.item.file.transform.DelimitedLineAggregator;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileSystemResource;
import org.springframework.jdbc.core.PreparedStatementSetter;

import javax.sql.DataSource;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.LocalDateTime;

@RequiredArgsConstructor
@Configuration
public class BatchConfig {

    private final JobBuilderFactory jobBuilderFactory;
    private final StepBuilderFactory stepBuilderFactory;
    private final DataSource dataSource;

    @Bean
    public Job job() throws SQLException {
        return jobBuilderFactory.get("job")
                .incrementer(new RunIdIncrementer())
                .start(step())
                .build();
    }

    @Bean
    public Step step() throws SQLException {
        return stepBuilderFactory.get("step")
                .<Transaction,Transaction>chunk(5)
                .reader(reader())
                .writer(new MultipleFileItemWriter())
                .processor(processor())
                .build();
    }

    @Bean
    public ItemReader<Transaction> reader() throws SQLException {
        JdbcCursorItemReader<Transaction> reader = new JdbcCursorItemReader<>();
        reader.setSql("select * from transaction where created_at between ? and ?");
        reader.setPreparedStatementSetter(ps -> {
            ps.setTimestamp(1, Timestamp.valueOf(LocalDateTime.now().minusDays(100)));
            ps.setTimestamp(2, Timestamp.valueOf(LocalDateTime.now()));
        });
        reader.setDataSource(dataSource);
        reader.setRowMapper((rs,rowNum) ->
            new Transaction(rs.getLong("id"),
                            rs.getString("account_no"),
                            rs.getString("transaction_type"),
                            rs.getBigDecimal("amount"),
                            rs.getString("description"))

        );
        return reader;
    }

    @Bean
    public ItemWriter<Transaction> writer(){
        FlatFileItemWriter<Transaction> writer = new FlatFileItemWriter<>();
        writer.setResource(new FileSystemResource("output.txt"));
        writer.setLineAggregator(new DelimitedLineAggregator<>() {{
            setDelimiter(",");
            setFieldExtractor(new BeanWrapperFieldExtractor<>() {{
                setNames(new String[]{"id", "accountNo", "transactionType", "amount", "description"});
            }});
        }});
        return writer;
    }

    @Bean
    public ItemProcessor<Transaction,Transaction> processor(){
        return item -> item;

    }
}
