package microservices.october;

import org.postgresql.jdbc2.optional.SimpleDataSource;
import org.springframework.jdbc.core.JdbcTemplate;

import java.sql.SQLException;
import java.util.Collection;

final class RoomRepository {

    private final JdbcTemplate jdbcTemplate;

    public RoomRepository() {
        SimpleDataSource dataSource = new SimpleDataSource();
        try {
            dataSource.setUrl("jdbc:postgresql://microservices-chris-10sep2014.cc9uedlzx2lk.eu-west-1.rds.amazonaws.com/micro");
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        dataSource.setUser("microservices");
        dataSource.setPassword("microservices");
        jdbcTemplate = new JdbcTemplate(dataSource);
    }

    public Collection<String> getRooms() {
        return jdbcTemplate.queryForList(
                "SELECT content FROM facts WHERE topic = 'someTopic'",
                String.class);
    }

    public static void main(String[] args) {
        System.out.println(new RoomRepository().getRooms());
    }
}