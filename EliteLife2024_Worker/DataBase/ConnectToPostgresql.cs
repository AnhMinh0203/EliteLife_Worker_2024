using Microsoft.Extensions.Configuration;
using Npgsql;
using System.Data;

namespace Elite_life_datacontext.DataBase
{
    public class ConnectToPostgresql
    {
        private readonly string _connectionString;

        public ConnectToPostgresql(IConfiguration configuration)
        {
            // Lấy chuỗi kết nối từ cấu hình
            _connectionString = configuration.GetConnectionString("PgDbConnection");

            // Kiểm tra chuỗi kết nối
            if (string.IsNullOrWhiteSpace(_connectionString))
            {
                throw new ArgumentException("Connection string for PostgreSQL is not provided or invalid.");
            }
        }

        /// <summary>
        /// Tạo kết nối đồng bộ tới PostgreSQL.
        /// </summary>
        /// <returns>IDbConnection</returns>
        public NpgsqlConnection CreateConnection()
        {
            var connection = new NpgsqlConnection(_connectionString);

            // Kiểm tra trạng thái kết nối
            if (connection.State != ConnectionState.Open)
            {
                connection.Open();
            }

            return connection;
        }

        /// <summary>
        /// Tạo kết nối bất đồng bộ tới PostgreSQL.
        /// </summary>
        /// <returns>Task<IDbConnection></returns>
        public async Task<NpgsqlConnection> CreateConnectionAsync()
        {
            var connection = new NpgsqlConnection(_connectionString);

            // Mở kết nối bất đồng bộ
            if (connection.State != ConnectionState.Open)
            {
                await connection.OpenAsync();
            }

            return connection;
        }
    }
}
