import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import org.apache.commons.pool2.BasePooledObjectFactory;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.impl.DefaultPooledObject;


public class RMQConnectionFactory extends BasePooledObjectFactory<Connection> {
    private final ConnectionFactory factory;

    public RMQConnectionFactory(ConnectionFactory factory) {
        this.factory = factory;
    }

    @Override
    public Connection create() throws Exception {
        return factory.newConnection();
    }

    @Override
    public PooledObject<Connection> wrap(Connection connection) {
        return new DefaultPooledObject<>(connection);
    }
}
