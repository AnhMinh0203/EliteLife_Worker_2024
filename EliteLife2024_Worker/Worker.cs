
using Dapper;
using Elite_life_datacontext.DataBase;
using EliteLife2024_Worker.Model;
using Microsoft.Extensions.Configuration;
using System.Data;
using System.Data.SqlClient;

namespace EliteLife2024_Worker
{
    public class Worker : BackgroundService
    {
        private readonly ILogger<Worker> _logger;
        private readonly IConfiguration _configuration;

        public Worker(ILogger<Worker> logger, IConfiguration configuration)
        {
            _logger = logger;
            _configuration = configuration;
        }

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            Console.OutputEncoding = System.Text.Encoding.UTF8;
            Console.WriteLine("Xin chào! Đây là thử nghiệm tiếng Việt: Đẹp trai và tài năng!");

            while (!stoppingToken.IsCancellationRequested)
            {
                try
                {
                    if (_logger.IsEnabled(LogLevel.Information))
                    {
                        // 1. Gọi hàm dbo.get_order() để lấy dữ liệu
                        var connectPostgres = new ConnectToPostgresql(_configuration);
                        using var connection = await connectPostgres.CreateConnectionAsync();

                        using var command = connection.CreateCommand();
                        command.CommandText = @"SELECT * FROM dbo.get_order();";
                        using var reader = await command.ExecuteReaderAsync(stoppingToken);

                        if (await reader.ReadAsync(stoppingToken))
                        {
                            var collaboratorId = reader.GetInt32(0);
                            // Ánh xạ dữ liệu từ hàm get_order vào model
                            var order = new CommissionModel
                            {
                                CollaboratorId = reader.GetInt32(0), 
                                AmountOrder = reader.GetInt32(1)   
                            };

                            var gratitude = new GratitudeCommissionModel
                            {
                                CollaboratorId = reader.GetInt32(0),
                                AmountOrder = reader.GetInt32(1),
                                OrderId = reader.GetInt32(2),
                            };

                            // 2. Gọi các hàm chia hoa hồng
                            var shareResult = await CaculateShareCommissionAsync(order);
                            _logger.LogInformation($"Kết quả chia hoa hồng đồng chia: {shareResult}");

                            var introResult = await CaculateIntroCommissionAsync(order);
                            _logger.LogInformation($"Kết quả chia hoa hồng giới thiệu: {introResult}");

                            var leaderResult = await CaculateLeaderCommissionAsync(order);
                            _logger.LogInformation($"Kết quả chia hoa hồng lãnh đạo: {leaderResult}");

                            var gratitudeResult = await CaculateGratitudeCommissionAsync(gratitude);
                            _logger.LogInformation($"Kết quả chia hoa hồng tri ân: {gratitudeResult}");

                            //3. Cập nhật Rank
                            var rankResult = await CheckRankAncestorsAsync(collaboratorId);
                            _logger.LogInformation($"Kết quả kiểm tra rank: {rankResult}");


                            //3. Cập nhật Star
                            var starResult = await CheckStarAncestorsAsync(collaboratorId);
                            _logger.LogInformation($"Kết quả kiểm tra star: {starResult}");

                            //4. Cập nhật IsProcess cho Order
                            var orderStatusResult = await UpdateOrderStatusAsync(collaboratorId);
                            _logger.LogInformation($"Kết quả kiểm tra status: {orderStatusResult}");

                        }
                        else
                        {
                            _logger.LogInformation("Không có bản ghi nào phù hợp từ hàm dbo.get_order().");
                        }
                    }
                }
                catch (Exception ex)
                {
                    _logger.LogError($"Lỗi khi xử lý: {ex.Message}");
                }
                // sau 1s nó chạy lại 1 lần
                await Task.Delay(15000, stoppingToken);
            }
        }

        // Đồng chia
        public async Task<string> CaculateShareCommissionAsync(CommissionModel shareCommissionModel)
        {
            var connectPostgres = new ConnectToPostgresql(_configuration);
            using var connection = await connectPostgres.CreateConnectionAsync();

            try
            {
                using var command = connection.CreateCommand();
                command.CommandText = @"SELECT * FROM dbo.cal_share_commission(
                     @p_collaboratorId, 
                     @p_amountOrder 
                     )";

                command.Parameters.AddWithValue("@p_collaboratorId", shareCommissionModel.CollaboratorId);
                command.Parameters.AddWithValue("@p_amountOrder", shareCommissionModel.AmountOrder);

                var result = (string)await command.ExecuteScalarAsync();
                return result;

            }
            catch (Exception ex)
            {
                return $"Lỗi khi xử lý: {ex.Message}";
            }
            finally
            {
                await connection.CloseAsync();
            }
        }

        // Giới thiệu
        public async Task<string> CaculateIntroCommissionAsync(CommissionModel introCommissionModel)
        {
            var connectPostgres = new ConnectToPostgresql(_configuration);
            using var connection = await connectPostgres.CreateConnectionAsync();

            try
            {
                using var command = connection.CreateCommand();
                command.CommandText = @"SELECT * FROM dbo.cal_intro_commission(
                    @p_collaboratorid, 
                    @p_amountOrder 
                    )";

                command.Parameters.AddWithValue("@p_collaboratorId", introCommissionModel.CollaboratorId);
                command.Parameters.AddWithValue("@p_amountOrder", introCommissionModel.AmountOrder);

                var result = (string)await command.ExecuteScalarAsync();
                return result;

            }
            catch (Exception ex)
            {
                return $"Lỗi khi xử lý: {ex.Message}";
            }
            finally
            {
                await connection.CloseAsync();
            }
        }

        // Lãnh đạo
        public async Task<string> CaculateLeaderCommissionAsync(CommissionModel introCommissionModel)
        {
            var connectPostgres = new ConnectToPostgresql(_configuration);
            using var connection = await connectPostgres.CreateConnectionAsync();

            //await using var transaction = await connection.BeginTransactionAsync();
            try
            {
                // 1. Lấy danh sách các cộng tác viên và tổng số lượng theo Rank
                var collaborators = await connection.QueryAsync<CollaboratorCommission>(
                    @"SELECT ""Id"", ""Rank"" 
                      FROM dbo.""Collaborators"" 
                      WHERE ""Rank"" IN ('V1', 'V2', 'V3', 'V4', 'V5') 
                      ORDER BY ""Id"";"
                );

                if (!collaborators.Any())
                {
                    return "Không có cộng tác viên phù hợp để tính hoa hồng.";
                }

                // Tính tổng số lượng từng Rank
                var totalByRank = new Dictionary<string, int>
                {
                    { "V1", collaborators.Count(c => c.Rank == "V1") },
                    { "V2", collaborators.Count(c => c.Rank == "V2") },
                    { "V3", collaborators.Count(c => c.Rank == "V3") },
                    { "V4", collaborators.Count(c => c.Rank == "V4") },
                    { "V5", collaborators.Count(c => c.Rank == "V5") }
                };

                // 2. Tính hoa hồng theo từng Rank
                var baseCommission = 3450000 * introCommissionModel.AmountOrder;
                var commissionByRank = new Dictionary<string, decimal>
                {
                    { "V1", Math.Round(baseCommission * 0.6M / totalByRank["V1"]) },
                    { "V2", Math.Round(baseCommission * 0.3M / totalByRank["V2"]) },
                    { "V3", Math.Round(baseCommission * 0.2M / totalByRank["V3"]) },
                    { "V4", Math.Round(baseCommission * 0.1M / totalByRank["V4"]) },
                    { "V5", Math.Round(baseCommission * 0.1M / totalByRank["V5"]) }
                };

                // 3. Duyệt qua từng cộng tác viên để cập nhật
                foreach (var collaborator in collaborators)
                {
                    var rank = collaborator.Rank;
                    if (!commissionByRank.ContainsKey(rank)) continue;

                    var updateCommission = commissionByRank[rank];

                    // Cập nhật ví
                    await connection.ExecuteAsync(
                        @"UPDATE dbo.""Wallets""
                  SET ""Available"" = COALESCE(""Available"", 0) + @UpdateCommission
                  WHERE ""CollaboratorId"" = @CollaboratorId AND ""WalletTypeEnums"" = 'Sale2';",
                        new { UpdateCommission = updateCommission, CollaboratorId = collaborator.Id }
                    );

                    // Cập nhật ngưỡng đã nhận
                    await connection.ExecuteAsync(
                        @"UPDATE dbo.""Collaborators""
                  SET ""Sale2Received"" = COALESCE(""Sale2Received"", 0) + @UpdateCommission
                  WHERE ""Id"" = @CollaboratorId;",
                        new { UpdateCommission = updateCommission, CollaboratorId = collaborator.Id }
                    );

                    // Gọi hàm `create_wallet_history` để ghi lịch sử
                    await connection.ExecuteAsync(
                        @"SELECT dbo.create_wallet_history(
                            @CollaboratorId,
                            'Sale2',
                            @Value,
                            @Note
                          );",
                        new
                        {
                            CollaboratorId = collaborator.Id,
                            Value = updateCommission,
                            Note = $"Hoa hồng lãnh đạo từ cộng tác viên EL{introCommissionModel.CollaboratorId} mua {introCommissionModel.AmountOrder} combo"
                        }
                    );
                }
                //await transaction.CommitAsync();
                return "Cập nhật hoa hồng lãnh đạo thành công.";
            }
            catch (Exception ex)
            {
                return $"Lỗi khi xử lý: {ex.Message}";
            }
            finally
            {
                await connection.CloseAsync();
            }
        }

        // Tri ân
        public async Task<string> CaculateGratitudeCommissionAsync(GratitudeCommissionModel gratitudeCommissionModel)
        {
            var connectPostgres = new ConnectToPostgresql(_configuration);
            using var connection = await connectPostgres.CreateConnectionAsync();

            var maxLevel = 21;
            List<int> listOrdersId = new();
            for (int i = 0; i < maxLevel; i++)
            {
                if (gratitudeCommissionModel.OrderId % 2 != 0)
                {
                    gratitudeCommissionModel.OrderId = (gratitudeCommissionModel.OrderId - 1) / 2;
                }
                else
                {
                    gratitudeCommissionModel.OrderId = gratitudeCommissionModel.OrderId / 2;
                }

                if (gratitudeCommissionModel.OrderId >= 1)
                {
                    listOrdersId.Add(gratitudeCommissionModel.OrderId);
                }
                else
                {
                    break;
                }
            }

            try
            {
                using var command = connection.CreateCommand();
                command.CommandText = @"SELECT * FROM dbo.cal_gratitude_commission(
                    @p_collaboratorId,
                    @p_amountOrder,
                    @p_ordersId
                    )";

                command.Parameters.AddWithValue("@p_collaboratorId", gratitudeCommissionModel.CollaboratorId);
                command.Parameters.AddWithValue("@p_amountOrder", gratitudeCommissionModel.AmountOrder);
                command.Parameters.AddWithValue("@p_ordersId", listOrdersId.ToArray());

                var result = (string)await command.ExecuteScalarAsync();
                return result;
            }
            catch (Exception ex)
            {
                return $"Lỗi khi xử lý: {ex.Message}";
            }
            finally
            {
                await connection.CloseAsync();
            }
        }

        // Cập nhật rank cho 1 thằng cha
        public async Task<string> CheckRankAsync(int collaboratorId)
        {
            var connectPostgres = new ConnectToPostgresql(_configuration);
            using var connection = await connectPostgres.CreateConnectionAsync();

            try
            {
                var query = @"Select * from dbo.check_rank (@collaboratorId)";
                var result = await connection.ExecuteScalarAsync<string>(query, new { CollaboratorId = collaboratorId });

                return result;
            }
            catch (Exception ex)
            {
                throw new Exception($"Lỗi: {ex.Message}", ex);
            }
            finally
            {
                await connection.CloseAsync();
            }
        }

        // Cập nhật rank cho tất cả cha, ông
        public async Task<string> CheckRankAncestorsAsync(int collaboratorId)
        {
            var connectPostgres = new ConnectToPostgresql(_configuration);
            using var connection = await connectPostgres.CreateConnectionAsync();

            try
            {
                var query = @"Select * from dbo.get_ancestors (@collaboratorId)";
                var ancestors = await connection.QueryAsync<int>(query, new { CollaboratorId = collaboratorId });
                ancestors = ancestors.Append(collaboratorId);
                foreach (var ancestorId in ancestors)
                {
                    await CheckRankAsync(ancestorId);
                }
                return "Đã cập nhật rank cho các bậc cha.";

            }
            catch (Exception ex)
            {
                throw new Exception($"Lỗi: {ex.Message}", ex);
            }
            finally
            {
                await connection.CloseAsync();
            }
        }

        // Cập nhật star cho 1 thằng cha
        public async Task<string> CheckStarAsync(int collaboratorId)
        {
            var connectPostgres = new ConnectToPostgresql(_configuration);
            using var connection = await connectPostgres.CreateConnectionAsync();

            try
            {
                var query = @"Select * from dbo.check_star (@collaboratorId)";
                var result = await connection.ExecuteScalarAsync<string>(query, new { CollaboratorId = collaboratorId });

                return result;
            }
            catch (Exception ex)
            {
                throw new Exception($"Lỗi: {ex.Message}", ex);
            }
            finally
            {
                await connection.CloseAsync();
            }
        }

        // Cập nhật star cho tất cả cha, ông

        public async Task<string> CheckStarAncestorsAsync(int collaboratorId)
        {
            var connectPostgres = new ConnectToPostgresql(_configuration);
            using var connection = await connectPostgres.CreateConnectionAsync();

            try
            {
                var query = @"Select * from dbo.get_ancestors (@collaboratorId)";
                var ancestors = await connection.QueryAsync<int>(query, new { CollaboratorId = collaboratorId });
                ancestors = ancestors.Append(collaboratorId);
                foreach (var ancestorId in ancestors)
                {
                    await CheckStarAsync(ancestorId);
                }
                return "Đã cập nhật sao cho các bậc cha.";

            }
            catch (Exception ex)
            {
                throw new Exception($"Lỗi: {ex.Message}", ex);
            }
            finally
            {
                await connection.CloseAsync();
            }
        }

        // Cập nhật order status
        public async Task<string> UpdateOrderStatusAsync(int collaboratorId)
        {
            var connectPostgres = new ConnectToPostgresql(_configuration);
            using var connection = await connectPostgres.CreateConnectionAsync();

            try
            {
                var query = @"Select * from dbo.update_status_order (@collaboratorId)";
                var result = await connection.ExecuteScalarAsync<string>(query, new { CollaboratorId = collaboratorId });

                return result;
            }
            catch (Exception ex)
            {
                throw new Exception($"Lỗi: {ex.Message}", ex);
            }
            finally
            {
                await connection.CloseAsync();
            }
        }

    }
}
