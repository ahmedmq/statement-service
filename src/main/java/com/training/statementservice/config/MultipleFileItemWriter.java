package com.training.statementservice.config;

import com.training.statementservice.domain.Transaction;
import org.springframework.batch.item.*;
import org.springframework.batch.item.file.FlatFileItemWriter;
import org.springframework.batch.item.file.transform.BeanWrapperFieldExtractor;
import org.springframework.batch.item.file.transform.DelimitedLineAggregator;
import org.springframework.core.io.UrlResource;

import java.net.MalformedURLException;
import java.time.LocalDate;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MultipleFileItemWriter implements ItemStream, ItemWriter<Transaction> {

    private Map<String, FlatFileItemWriter<Transaction>> writers = new HashMap<>();


    private ExecutionContext executionContext;


    @Override
    public void open(ExecutionContext executionContext) throws ItemStreamException {
        this.executionContext = executionContext;
    }

    @Override
    public void update(ExecutionContext executionContext) throws ItemStreamException {

    }

    @Override
    public void close() throws ItemStreamException {
        for (FlatFileItemWriter<Transaction> fileItemWriter: writers.values()){
          //  fileItemWriter.close();
        }
    }

    @Override
    public void write(List<? extends Transaction> items) throws Exception {
        for (Transaction transaction: items){
            FlatFileItemWriter<Transaction> ff = getFlatFileItemWriter(transaction);
            ff.write(Arrays.asList(transaction));
        }
    }

    private FlatFileItemWriter<Transaction> getFlatFileItemWriter(Transaction transaction) throws MalformedURLException {
        String key = transaction.getAccountNo();
        FlatFileItemWriter<Transaction> ff = writers.get(key);
        if (ff == null){
            ff = new FlatFileItemWriter<Transaction>();
                UrlResource resource = new UrlResource("file:"+ key+".txt");
                ff.setResource(resource);
                ff.open(executionContext);
                ff.setLineAggregator(new DelimitedLineAggregator<>() {{
                    setDelimiter(",");
                    setFieldExtractor(new BeanWrapperFieldExtractor<>() {{
                        setNames(new String[]{"id", "accountNo", "transactionType", "amount", "description"});
                    }});
                }});
                writers.put(key,ff);
        }

        return ff;
    }

}
