package dev.rinaldo;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.util.Queue;
import java.util.Random;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import javax.inject.Inject;
import javax.sql.DataSource;

import org.eclipse.microprofile.context.ManagedExecutor;

import io.quarkus.runtime.Quarkus;
import io.quarkus.runtime.QuarkusApplication;

public class MultiThread implements QuarkusApplication {

	// fechar recursos
	// limitar qtd de tarefas em paralelo
	// melhorar o clear
	// melhorar tratamento de exceção
	// quartz/scheduler
	
	@Inject
	DataSource dataSource;
	
	@Inject
	ManagedExecutor executor;
	
	public static void main(String[] args) {
		Quarkus.run(MultiThread.class);
		Quarkus.asyncExit();
	}
	
	@Override
	public int run(String... args) throws Exception {
		populaBanco();
    	
    	Instant inicio = Instant.now();
		Supplier<Integer> tarefa = new Supplier<Integer>() {
            @Override
            public Integer get() {
            	try (Connection connection = dataSource.getConnection()) {
            		System.out.println("Iniciando tarefa.");
            		connection.setAutoCommit(false);
            		
            		String sqlSelect = "SELECT * FROM Compra WHERE situacao = 1 LIMIT 1 FOR UPDATE SKIP LOCKED;";
            		final PreparedStatement psSelect = connection.prepareStatement(sqlSelect);
            		String sqlUpdate = "UPDATE Compra SET situacao = 2 WHERE id = ?;";
            		final PreparedStatement psUpdate = connection.prepareStatement(sqlUpdate);
            		
            		ResultSet rsCompra = psSelect.executeQuery();
            		if (!rsCompra.next()) {
            			return -1;
            		}
            		long idCompra = rsCompra.getLong(1);
            		System.out.println("Tratando compra " + idCompra);
            		
            		TimeUnit.MILLISECONDS.sleep(new Random().nextInt(1000));
            		
            		psUpdate.setLong(1, idCompra);
            		psUpdate.executeUpdate();
            		connection.commit();
            		System.out.println("Compra " + idCompra + " tratada.");
            		
            		return 0;
            	} catch (Exception e) {
            		throw new RuntimeException(e);
            	}
            }
        };
    	
		final Queue<Integer> codigosRetornoTarefas = new PriorityBlockingQueue<>();
		while (true) {
			executor.supplyAsync(tarefa).thenAccept(code -> codigosRetornoTarefas.add(code));
			TimeUnit.MILLISECONDS.sleep(100);
			Integer poll = codigosRetornoTarefas.poll();
			if (poll != null && poll == -1) {
				break;
			} else {
				codigosRetornoTarefas.clear();
			}
		}
		
		System.out.println("Acabou.");
    	System.out.println(Duration.between(Instant.now(), inicio));
		return 0;
	}	
	
	private void populaBanco() throws SQLException {
		try (Connection connection = dataSource.getConnection()) {
			try (PreparedStatement ps = connection.prepareStatement("CREATE TABLE Compra (id SERIAL, situacao SMALLINT);")) {
				ps.execute();
			}
			for (int i = 0; i < 100; i++) {
				try (PreparedStatement ps = connection.prepareStatement("INSERT INTO Compra (situacao) values (1);")) {
					ps.execute();
				}
			}
			try (PreparedStatement ps = connection.prepareStatement("SELECT COUNT(*) FROM Compra")) {
				ResultSet rs = ps.executeQuery();
				rs.next();
				long count = rs.getLong(1);
				System.out.println("COUNT: " + count);
			}
		}
	}

}