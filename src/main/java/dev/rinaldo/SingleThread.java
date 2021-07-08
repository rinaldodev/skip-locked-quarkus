package dev.rinaldo;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import javax.inject.Inject;
import javax.sql.DataSource;

import io.quarkus.runtime.Quarkus;
import io.quarkus.runtime.QuarkusApplication;

public class SingleThread implements QuarkusApplication {

	@Inject
	DataSource dataSource;
	
	public static void main(String[] args) {
		Quarkus.run(SingleThread.class);
		Quarkus.asyncExit();
	}
	
	@Override
	public int run(String... args) throws Exception {
		populaBanco();
    	
    	Instant inicio = Instant.now();
    	try (Connection connection = dataSource.getConnection()) {
    		connection.setAutoCommit(false);
    		
    		String sqlSelect = "SELECT * FROM Compra WHERE situacao = 1 LIMIT 1 FOR UPDATE;";
			PreparedStatement psSelect = connection.prepareStatement(sqlSelect);
    		
			String sqlUpdate = "UPDATE Compra SET situacao = 2 WHERE id = ?;";
			PreparedStatement psUpdate = connection.prepareStatement(sqlUpdate);
    		
			while (true) {
				ResultSet rsCompra = psSelect.executeQuery();
				if (!rsCompra.next()) {
					break;
				}
				long id = rsCompra.getLong(1);
				System.out.println("Tratando compra " + id);
				
				TimeUnit.MILLISECONDS.sleep(new Random().nextInt(1000)); // simula processamento
				
				psUpdate.setLong(1, id);
				psUpdate.executeUpdate();
				connection.commit();
				System.out.println("Compra " + id + " tratada.");
    		}
			
    		System.out.println("Acabou.");
		}
    	
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