using Microsoft.AspNetCore.Mvc;
using System.Data.SqlClient;

namespace InquiryApi.Controllers
{


    [ApiController]
    [Route("api/[controller]")]

    public class ContainerController : ControllerBase
    {

        private readonly IConfiguration _configuration;
        public ContainerController(IConfiguration configuration)
        {
            _configuration = configuration;

        }


        [HttpGet("details/{containerNo}")]
        public async Task<ActionResult> GetContainerDetails(string containerNo)
        {
            using (SqlConnection connection = new SqlConnection(_configuration.GetConnectionString("DefaultConnection")))
            {
                await connection.OpenAsync();

                var sql = @"
                        SELECT cfs.ContainerNo, cfs.VirNo AS IGMNo, cfs.BLNo, cfs.IndexNo,
                               sh.Name AS Line, cfs.Size,
                               CONCAT(cfs.Status, '/', cfs.CargoType) AS SizeType,
                               gh.GoodsHeadName AS Category,
                               cfs.Description AS Goods_description,
                               pt.PortName AS Port_of_discharge,
                               cfs.CFSContainerGateInDate AS Arrival_date,
                               cfs.CFSContainerGateOutDate AS Deliver_date,
                               ve.VesselName AS Vessel,
                               vo.VoyageNo AS Voyage,
                               cfs.ManifestedSealNumber AS ShipperSeal,
                               i.OutTime AS Port_discharge_date,
                               cfs.ContainerGrossWeight AS Gross_weight
                        FROM ContainerIndex cfs
                        INNER JOIN ShippingAgent sh ON cfs.ShippingAgentId = sh.ShippingAgentId
                        INNER JOIN GoodsHead gh ON cfs.GoodsHeadId = gh.GoodsHeadId
                        INNER JOIN PortAndTerminal pt ON cfs.PortAndTerminalId = pt.PortAndTerminalId
                        INNER JOIN Voyage vo ON cfs.VoyageId = vo.VoyageId
                        INNER JOIN Vessel ve ON vo.VesselId = ve.VesselId
                        INNER JOIN IPAOs i ON cfs.VirNo = i.VIRNumber AND cfs.ContainerNo = i.ContainerNumber
                        WHERE cfs.ContainerNo = @ContainerNo

                        UNION ALL

                        SELECT cy.ContainerNo, cy.VirNo AS IGMNo, cy.BLNo, cy.IndexNo,
                               sh.Name AS Line, cy.Size,
                               CONCAT(cy.Status, '/', cy.CargoType) AS SizeType,
                               gh.GoodsHeadName AS Category,
                               cy.Description AS Goods_description,
                               pt.PortName AS Port_of_discharge,
                               cy.CYContainerGateInDate AS Arrival_date,
                               cy.CYContainerGateOutDate AS Deliver_date,
                               ve.VesselName AS Vessel,
                               vo.VoyageNo AS Voyage,
                               cy.ManifestedSealNumber AS ShipperSeal,
                               i.OutTime AS Port_discharge_date,
                               cy.ContainerGrossWeight AS Gross_weight
                        FROM CYContainer cy
                        INNER JOIN ShippingAgent sh ON cy.ShippingAgentId = sh.ShippingAgentId
                        INNER JOIN GoodsHead gh ON cy.GoodsHeadId = gh.GoodsHeadId
                        INNER JOIN PortAndTerminal pt ON cy.PortAndTerminalId = pt.PortAndTerminalId
                        INNER JOIN Voyage vo ON cy.VoyageNo = vo.VoyageNo
                        INNER JOIN Vessel ve ON vo.VesselId = ve.VesselId
                        INNER JOIN IPAOs i ON cy.VirNo = i.VIRNumber AND cy.ContainerNo = i.ContainerNumber
                        WHERE cy.ContainerNo = @ContainerNo;";

                using (var command = new SqlCommand(sql, connection))
                {
                    command.Parameters.AddWithValue("@ContainerNo", containerNo);

                    using (var reader = await command.ExecuteReaderAsync())
                    {
                        while (await reader.ReadAsync())
                        {
                            var result = new
                            {
                                ContainerNo = reader["ContainerNo"],
                                IGMNo = reader["IGMNo"],
                                BLNo = reader["BLNo"],
                                IndexNo = reader["IndexNo"],
                                Line = reader["Line"],
                                Size = reader["Size"],
                                SizeType = reader["SizeType"],
                                Category = reader["Category"],
                                GoodsDescription = reader["Goods_description"],
                                PortOfDischarge = reader["Port_of_discharge"],
                                ArrivalDate = reader["Arrival_date"],
                                DeliverDate = reader["Deliver_date"],
                                Vessel = reader["Vessel"],
                                Voyage = reader["Voyage"],
                                ShipperSeal = reader["ShipperSeal"],
                                PortDischargeDate = reader["Port_discharge_date"],
                                GrossWeight = reader["Gross_weight"]
                            };

                            // Create a new object with keys starting with capital letters
                            var capitalizedResult = new
                            {
                                ContainerNo = result.ContainerNo,
                                IgmNo = result.IGMNo,
                                BlNo = result.BLNo,
                                IndexNo = result.IndexNo,
                                Line = result.Line,
                                Size = result.Size,
                                SizeType = result.SizeType,
                                Category = result.Category,
                                GoodsDescription = result.GoodsDescription,
                                PortOfDischarge = result.PortOfDischarge,
                                ArrivalDate = result.ArrivalDate,
                                DeliverDate = result.DeliverDate,
                                Vessel = result.Vessel,
                                Voyage = result.Voyage,
                                ShipperSeal = result.ShipperSeal,
                                PortDischargeDate = result.PortDischargeDate,
                                GrossWeight = result.GrossWeight
                            };

                            return Ok(capitalizedResult);
                        }
                        // Return 200 OK with capitalized result

                    }
                }
            }


            return NotFound(); // Return 404 Not Found if no data is found
        }

        [HttpGet("by-container-no/{containerNo}")]
        public async Task<IActionResult> InquiryByContainerNo(string containerNo)
        {
            if (string.IsNullOrEmpty(containerNo))
            {
                return BadRequest("Container number cannot be null or empty.");
            }

            var results = new List<dynamic>();

            using (var connection = new SqlConnection(_configuration.GetConnectionString("DefaultConnection")))
            {
                await connection.OpenAsync();

                var sql = @"
                SELECT * FROM (
                    SELECT TOP 1 cfs.ContainerNo, cfs.VirNo AS IGMNo,
                           sh.Name AS Line, cfs.Size,
                           CONCAT(cfs.Status, '/', cfs.CargoType) AS SizeType,
                           gh.GoodsHeadName AS Category,
                           cfs.Description AS Goods_description,
                           pt.PortName AS Port_of_discharge,
                           cfs.CFSContainerGateInDate AS Arrival_date,
                           cfs.CFSContainerGateOutDate AS Deliver_date,
                           ve.VesselName AS Vessel,
                           vo.VoyageNo AS Voyage,
                           cfs.ManifestedSealNumber AS ShipperSeal,
                           i.OutTime AS Port_discharge_date,
                           cfs.ContainerGrossWeight AS Gross_weight
                    FROM ContainerIndex cfs
                    INNER JOIN ShippingAgent sh ON cfs.ShippingAgentId = sh.ShippingAgentId
                    INNER JOIN GoodsHead gh ON cfs.GoodsHeadId = gh.GoodsHeadId
                    INNER JOIN PortAndTerminal pt ON cfs.PortAndTerminalId = pt.PortAndTerminalId
                    INNER JOIN Voyage vo ON cfs.VoyageId = vo.VoyageId
                    INNER JOIN Vessel ve ON vo.VesselId = ve.VesselId
                    INNER JOIN IPAOs i ON cfs.VirNo = i.VIRNumber AND cfs.ContainerNo = i.ContainerNumber
                    WHERE cfs.ContainerNo = @ContainerNo
                    ORDER BY cfs.CFSContainerGateInDate DESC
                ) AS LatestContainer

                UNION ALL

                SELECT * FROM (
                    SELECT TOP 1 cy.ContainerNo, cy.VirNo AS IGMNo,
                           sh.Name AS Line, cy.Size,
                           CONCAT(cy.Status, '/', cy.CargoType) AS SizeType,
                           gh.GoodsHeadName AS Category,
                           cy.Description AS Goods_description,
                           pt.PortName AS Port_of_discharge,
                           cy.CYContainerGateInDate AS Arrival_date,
                           cy.CYContainerGateOutDate AS Deliver_date,
                           ve.VesselName AS Vessel,
                           vo.VoyageNo AS Voyage,
                           cy.ManifestedSealNumber AS ShipperSeal,
                           i.OutTime AS Port_discharge_date,
                           cy.ContainerGrossWeight AS Gross_weight
                    FROM CYContainer cy
                    INNER JOIN ShippingAgent sh ON cy.ShippingAgentId = sh.ShippingAgentId
                    INNER JOIN GoodsHead gh ON cy.GoodsHeadId = gh.GoodsHeadId
                    INNER JOIN PortAndTerminal pt ON cy.PortAndTerminalId = pt.PortAndTerminalId
                    INNER JOIN Voyage vo ON cy.VoyageNo = vo.VoyageNo
                    INNER JOIN Vessel ve ON vo.VesselId = ve.VesselId
                    INNER JOIN IPAOs i ON cy.VirNo = i.VIRNumber AND cy.ContainerNo = i.ContainerNumber
                    WHERE cy.ContainerNo = @ContainerNo
                    ORDER BY cy.CYContainerGateInDate DESC
                ) AS LatestCY;";

                using (var command = new SqlCommand(sql, connection))
                {
                    command.Parameters.AddWithValue("@ContainerNo", containerNo);

                    using (var reader = await command.ExecuteReaderAsync())
                    {
                        while (await reader.ReadAsync())
                        {
                            var result = new
                            {
                                ContainerNo = reader["ContainerNo"],
                                IGMNo = reader["IGMNo"],
                                Line = reader["Line"],
                                Size = reader["Size"],
                                SizeType = reader["SizeType"],
                                Category = reader["Category"],
                                GoodsDescription = reader["Goods_description"],
                                PortOfDischarge = reader["Port_of_discharge"],
                                ArrivalDate = reader["Arrival_date"] as DateTime?,
                                DeliverDate = reader["Deliver_date"] as DateTime?,
                                Vessel = reader["Vessel"],
                                Voyage = reader["Voyage"],
                                ShipperSeal = reader["ShipperSeal"],
                                PortDischargeDate = reader["Port_discharge_date"] as DateTime?,
                                GrossWeight = reader["Gross_weight"]
                            };

                            results.Add(result);
                        }
                    }
                }
            }

            if (results.Count > 0)
            {
                return Ok(results); // Return 200 OK with the results
            }

            return NotFound("No data found for the specified container number."); // Handle case where no data is found
        }

        [HttpGet("by-blno/{blno}")]
        public async Task<IActionResult> InquiryByBLNo(string blno)
        {
            // Validate input
            if (string.IsNullOrEmpty(blno))
            {
                return BadRequest("BL number cannot be null or empty.");
            }

            var results = new List<dynamic>();

            using (var connection = new SqlConnection(_configuration.GetConnectionString("DefaultConnection")))
            {
                await connection.OpenAsync();

                var sql = @"
                SELECT 
                    cy.Status AS Type, 
                    cy.CFSContainerGateInDate AS ArrivalDate, 
                    cy.ContainerNo, 
                    cy.BLNo, 
                    cy.VirNo AS IGM, 
                    cy.IndexNo, 
                    gh.GoodsHeadName, 
                    cy.IsDestuffed AS Destuffed
                FROM ContainerIndex cy
                INNER JOIN GoodsHead gh ON cy.GoodsHeadId = gh.GoodsHeadId
                WHERE cy.BLNo = @blno
                
                UNION ALL
                
                SELECT 
                    cy.Status AS Type, 
                    cy.CYContainerGateInDate AS ArrivalDate, 
                    cy.ContainerNo, 
                    cy.BLNo, 
                    cy.VirNo AS IGM, 
                    cy.IndexNo, 
                    gh.GoodsHeadName, 
                    cy.IsDestuffed AS Destuffed
                FROM CYContainer cy
                INNER JOIN GoodsHead gh ON cy.GoodsHeadId = gh.GoodsHeadId
                WHERE cy.BLNo = @blno";

                using (var command = new SqlCommand(sql, connection))
                {
                    command.Parameters.AddWithValue("@blno", blno);

                    using (var reader = await command.ExecuteReaderAsync())
                    {
                        while (await reader.ReadAsync())
                        {
                            var result = new
                            {
                                Type = reader["Type"],
                                ArrivalDate = reader["ArrivalDate"] as DateTime?,
                                ContainerNo = reader["ContainerNo"],
                                BLNo = reader["BLNo"],
                                IGM = reader["IGM"],
                                IndexNo = reader["IndexNo"],
                                GoodsHeadName = reader["GoodsHeadName"],
                                IsDestuffed = reader["Destuffed"]
                            };

                            results.Add(result);
                        }
                    }
                }
            }

            // Check if any results were found
            if (results.Count > 0)
            {
                return Ok(results); // Return 200 OK with the results
            }

            return NotFound("No data found for the specified BL number."); // Return 404 if no data found
        }



        [HttpGet("amount-by-virno-indexno")]
        public async Task<IActionResult> GetAmountByVirNoOrIndexNo(string virNo, int indexNo)
        {
            // Validate input parameters
            if (string.IsNullOrWhiteSpace(virNo) && indexNo <= 0)
            {
                return BadRequest();
            }
             
            long? amount = null;

            try
            {
                using (var connection = new SqlConnection(_configuration.GetConnectionString("DefaultConnection")))
                {
                    await connection.OpenAsync();

                    // Query for amounts from ContainerIndex
                    var sqlContainer = @"
                SELECT i.Ammount 
                FROM InvoiceInquiry i
                JOIN ContainerIndex c ON i.ContainerIndexId = c.ContainerIndexId
                WHERE c.VirNo = @virNo AND c.IndexNo = @indexNo";

                    using (var command = new SqlCommand(sqlContainer, connection))
                    {
                        command.Parameters.AddWithValue("@virNo", virNo);
                        command.Parameters.AddWithValue("@indexNo", indexNo);

                        amount = await command.ExecuteScalarAsync() as long?;
                    }

                    // If no amount found in ContainerIndex, check CYContainer
                    if (!amount.HasValue || amount.Value <= 0)
                    {
                        var sqlCY = @"
                    SELECT i.Ammount 
                    FROM InvoiceInquiry i
                    JOIN CYContainer cy ON i.CYContainerId = cy.CYContainerId
                    WHERE cy.VirNo = @virNo AND cy.IndexNo = @indexNo";

                        using (var command = new SqlCommand(sqlCY, connection))
                        {
                            command.Parameters.AddWithValue("@virNo", virNo);
                            command.Parameters.AddWithValue("@indexNo", indexNo);

                            amount = await command.ExecuteScalarAsync() as long?;
                        }
                    }
                }

                // Check if an amount was found
                if (amount.HasValue && amount.Value > 0)
                {
                    return Ok(new {Amount = amount.Value });
                }

                return NotFound();
            }
            catch (Exception ex)
            {
                // Log the exception if needed
                return NotFound();
            }
        }

        public class InquiryResult
        {
            public long Amount { get; set; }
            public string TillDate { get; set; } // Add other fields as necessary
        }

        [HttpGet("amount-by-virno-indexNo-FromDate")]
        public async Task<IActionResult> GetAmountByVirNoOrIndexNo(string virNo, int indexNo, string fromDate)
        {
            // Validate input parameters
            if (string.IsNullOrWhiteSpace(virNo) || indexNo <= 0 || string.IsNullOrWhiteSpace(fromDate))
            {
                return BadRequest("Invalid input parameters.");
            }

            InquiryResult inquiryResult = null;

            try
            {
                using (var connection = new SqlConnection(_configuration.GetConnectionString("DefaultConnection")))
                {
                    await connection.OpenAsync();

                    // Query for amounts and inquiry details from ContainerIndex
                    var sqlContainer = @"
            SELECT i.Ammount, i.InquiryAbout  -- Modify this to include the details you want
            FROM InvoiceInquiry i
            JOIN ContainerIndex c ON i.ContainerIndexId = c.ContainerIndexId
            WHERE c.VirNo = @virNo AND c.IndexNo = @indexNo AND i.InquiryAbout = @fromDate";

                    using (var command = new SqlCommand(sqlContainer, connection))
                    {
                        command.Parameters.AddWithValue("@virNo", virNo);
                        command.Parameters.AddWithValue("@indexNo", indexNo);
                        command.Parameters.AddWithValue("@fromDate", fromDate); // Add FromDate as a parameter

                        using (var reader = await command.ExecuteReaderAsync())
                        {
                            if (await reader.ReadAsync())
                            {
                                inquiryResult = new InquiryResult
                                {
                                    Amount = reader.GetInt64(0), // Assuming the amount is a long
                                    TillDate = reader.GetString(1) // Adjust based on your columns
                                };
                            }
                        }
                    }

                    // If no result found in ContainerIndex, check CYContainer
                    if (inquiryResult == null)
                    {
                        var sqlCY = @"
                SELECT i.Ammount, i.InquiryAbout  -- Modify this to include the details you want
                FROM InvoiceInquiry i
                JOIN CYContainer cy ON i.CYContainerId = cy.CYContainerId
                WHERE cy.VirNo = @virNo AND cy.IndexNo = @indexNo AND i.InquiryAbout = @fromDate";

                        using (var command = new SqlCommand(sqlCY, connection))
                        {
                            command.Parameters.AddWithValue("@virNo", virNo);
                            command.Parameters.AddWithValue("@indexNo", indexNo);
                            command.Parameters.AddWithValue("@fromDate", fromDate); // Add FromDate as a parameter

                            using (var reader = await command.ExecuteReaderAsync())
                            {
                                if (await reader.ReadAsync())
                                {
                                    inquiryResult = new InquiryResult
                                    {
                                        Amount = reader.GetInt64(0),
                                        TillDate = reader.GetString(1)
                                    };
                                }
                            }
                        }
                    }
                }

                // Check if an inquiry was found
                if (inquiryResult != null)
                {
                    return Ok(inquiryResult);
                }

                return NotFound("No inquiry found for the given parameters.");
            }
            catch (Exception ex)
            {
                // Log the exception as needed
                // For example, you could log ex.ToString() to your logging framework
                return StatusCode(500, "Internal server error.");
            }
        }


        [HttpGet("details")]
        public async Task<ActionResult> GetContainerDetails(string containerNo, string blNo)
        {
            using (SqlConnection connection = new SqlConnection(_configuration.GetConnectionString("DefaultConnection")))
            {
                await connection.OpenAsync();

                var sql = @"
            DECLARE @CargoType VARCHAR(10);
            DECLARE @ContainerNo VARCHAR(20) = @ContainerNoInput;
            DECLARE @BLNo VARCHAR(20) = @BLNoInput;
            DECLARE @Result TABLE (
                ContainerNo VARCHAR(20),
                IGMNo VARCHAR(20),
                BLNo VARCHAR(20),
                IndexNo INT,
                Line VARCHAR(50),
                Size VARCHAR(20),
                SizeType VARCHAR(50),
                Category VARCHAR(50),
                Goods_description VARCHAR(255),
                Port_of_discharge VARCHAR(50),
                Arrival_date DATETIME,
                Deliver_date DATETIME,
                Vessel VARCHAR(50),
                Voyage VARCHAR(50),
                ShipperSeal VARCHAR(50),
                Port_discharge_date DATETIME,
                Gross_weight DECIMAL(18,2)
               
            );

            -- Determine Cargo Type based on ContainerNo
            IF @ContainerNo IS NOT NULL
            BEGIN
                SELECT TOP 1 @CargoType = cfs.CargoType
                FROM ContainerIndex cfs
                WHERE cfs.ContainerNo = @ContainerNo;

                IF @CargoType IS NULL
                BEGIN
                    SELECT TOP 1 @CargoType = cy.CargoType
                    FROM CYContainer cy
                    WHERE cy.ContainerNo = @ContainerNo;
                END
            END

            -- If Cargo Type is still not determined, check based on BLNo
            IF @CargoType IS NULL AND @BLNo IS NOT NULL
            BEGIN
                SELECT TOP 1 @CargoType = cfs.CargoType
                FROM ContainerIndex cfs
                WHERE cfs.BLNo = @BLNo;

                IF @CargoType IS NULL
                BEGIN
                    SELECT TOP 1 @CargoType = cy.CargoType
                    FROM CYContainer cy
                    WHERE cy.BLNo = @BLNo;
                END
            END

            -- Insert results based on cargo type into the result table
            IF @CargoType = 'FCL'
            BEGIN
                -- Check in ContainerIndex (CFS) using ContainerNo
                INSERT INTO @Result
                SELECT TOP 1 cfs.ContainerNo, cfs.VirNo AS IGMNo, cfs.BLNo, cfs.IndexNo,
                       sh.Name AS Line, cfs.Size,
                       CONCAT(cfs.Status, '/', cfs.CargoType) AS SizeType,
                       gh.GoodsHeadName AS Category,
                       cfs.Description AS Goods_description,
                       pt.PortName AS Port_of_discharge,
                       cfs.CFSContainerGateInDate AS Arrival_date,
                       cfs.CFSContainerGateOutDate AS Deliver_date,
                       ve.VesselName AS Vessel,
                       vo.VoyageNo AS Voyage,
                       cfs.ManifestedSealNumber AS ShipperSeal,
                       i.OutTime AS Port_discharge_date,
                       cfs.ContainerGrossWeight AS Gross_weight
                FROM ContainerIndex cfs
                INNER JOIN ShippingAgent sh ON cfs.ShippingAgentId = sh.ShippingAgentId
                INNER JOIN GoodsHead gh ON cfs.GoodsHeadId = gh.GoodsHeadId
                INNER JOIN PortAndTerminal pt ON cfs.PortAndTerminalId = pt.PortAndTerminalId
                INNER JOIN Voyage vo ON cfs.VoyageId = vo.VoyageId
                INNER JOIN Vessel ve ON vo.VesselId = ve.VesselId
                INNER JOIN IPAOs i ON cfs.VirNo = i.VIRNumber AND cfs.ContainerNo = i.ContainerNumber
                WHERE cfs.ContainerNo = @ContainerNo;

                -- Check in CYContainer using ContainerNo
                IF @@ROWCOUNT = 0
                BEGIN
                    INSERT INTO @Result
                    SELECT TOP 1 cy.ContainerNo, cy.VirNo AS IGMNo, cy.BLNo, cy.IndexNo,
                           sh.Name AS Line, cy.Size,
                           CONCAT(cy.Status, '/', cy.CargoType) AS SizeType,
                           gh.GoodsHeadName AS Category,
                           cy.Description AS Goods_description,
                           pt.PortName AS Port_of_discharge,
                           cy.CYContainerGateInDate AS Arrival_date,
                           cy.CYContainerGateOutDate AS Deliver_date,
                           ve.VesselName AS Vessel,
                           vo.VoyageNo AS Voyage,
                           cy.ManifestedSealNumber AS ShipperSeal,
                           i.OutTime AS Port_discharge_date,
                           cy.ContainerGrossWeight AS Gross_weight
                    FROM CYContainer cy
                    INNER JOIN ShippingAgent sh ON cy.ShippingAgentId = sh.ShippingAgentId
                    INNER JOIN GoodsHead gh ON cy.GoodsHeadId = gh.GoodsHeadId
                    INNER JOIN PortAndTerminal pt ON cy.PortAndTerminalId = pt.PortAndTerminalId
                    INNER JOIN Voyage vo ON cy.VoyageNo = vo.VoyageNo
                    INNER JOIN Vessel ve ON vo.VesselId = ve.VesselId
                    INNER JOIN IPAOs i ON cy.VirNo = i.VIRNumber AND cy.ContainerNo = i.ContainerNumber
                    WHERE cy.ContainerNo = @ContainerNo;
                END
            END
            ELSE IF @CargoType = 'LCL'
            BEGIN
                -- Check in ContainerIndex (CFS) using BLNo
                INSERT INTO @Result
                                  SELECT TOP 1 
                        cfs.ContainerNo, 
                        cfs.VirNo AS IGMNo, 
                        cfs.BLNo, 
                        cfs.IndexNo,
                        sh.Name AS Line, 
                        cfs.Size,
                        CONCAT(cfs.Status, '/', cfs.CargoType) AS SizeType,
                        gh.GoodsHeadName AS Category,
                        cfs.Description AS Goods_description,
                        pt.PortName AS Port_of_discharge,
                        cfs.CFSContainerGateInDate AS Arrival_date,
                        cfs.CFSContainerGateOutDate AS Deliver_date,
                        ve.VesselName AS Vessel,
                        vo.VoyageNo AS Voyage,
                        cfs.ManifestedSealNumber AS ShipperSeal,
                        i.OutTime AS Port_discharge_date,
                        cfs.ContainerGrossWeight AS Gross_weight
                    FROM ContainerIndex cfs
                    INNER JOIN ShippingAgent sh ON cfs.ShippingAgentId = sh.ShippingAgentId
                    INNER JOIN GoodsHead gh ON cfs.GoodsHeadId = gh.GoodsHeadId
                    INNER JOIN PortAndTerminal pt ON cfs.PortAndTerminalId = pt.PortAndTerminalId
                    INNER JOIN Voyage vo ON cfs.VoyageId = vo.VoyageId
                    INNER JOIN Vessel ve ON vo.VesselId = ve.VesselId
                    INNER JOIN IPAOs i ON cfs.VirNo = i.VIRNumber AND cfs.ContainerNo = i.ContainerNumber
                    WHERE cfs.BLNo = @BLNo;

                -- Check in CYContainer using BLNo
                IF @@ROWCOUNT = 0
                BEGIN
                    INSERT INTO @Result
                    SELECT TOP 1 cy.ContainerNo, cy.VirNo AS IGMNo, cy.BLNo, cy.IndexNo,
                           sh.Name AS Line, cy.Size,
                           CONCAT(cy.Status, '/', cy.CargoType) AS SizeType,
                           gh.GoodsHeadName AS Category,
                           cy.Description AS Goods_description,
                           pt.PortName AS Port_of_discharge,
                           cy.CYContainerGateInDate AS Arrival_date,
                           cy.CYContainerGateOutDate AS Deliver_date,
                           ve.VesselName AS Vessel,
                           vo.VoyageNo AS Voyage,
                           cy.ManifestedSealNumber AS ShipperSeal,
                           i.OutTime AS Port_discharge_date,
                           cy.ContainerGrossWeight AS Gross_weight
                           
                    FROM CYContainer cy
                    INNER JOIN ShippingAgent sh ON cy.ShippingAgentId = sh.ShippingAgentId
                    INNER JOIN GoodsHead gh ON cy.GoodsHeadId = gh.GoodsHeadId
                    INNER JOIN PortAndTerminal pt ON cy.PortAndTerminalId = pt.PortAndTerminalId
                    INNER JOIN Voyage vo ON cy.VoyageNo = vo.VoyageNo
                    INNER JOIN Vessel ve ON vo.VesselId = ve.VesselId
                    INNER JOIN IPAOs i ON cy.VirNo = i.VIRNumber AND cy.ContainerNo = i.ContainerNumber
                    WHERE cy.BLNo = @BLNo;
                END
            END

            -- Select the final result
            SELECT * FROM @Result;
        ";

                using (var command = new SqlCommand(sql, connection))
                {
                    command.Parameters.AddWithValue("@ContainerNoInput", (object)containerNo ?? DBNull.Value);
                    command.Parameters.AddWithValue("@BLNoInput", (object)blNo ?? DBNull.Value);

                    using (var reader = await command.ExecuteReaderAsync())
                    {
                        if (await reader.ReadAsync())
                        {
                            var result = new
                            {
                                ContainerNo = reader["ContainerNo"],
                                IGMNo = reader["IGMNo"],
                                BLNo = reader["BLNo"],
                                IndexNo = reader["IndexNo"],
                                Line = reader["Line"],
                                Size = reader["Size"],
                                SizeType = reader["SizeType"],
                                Category = reader["Category"],
                                GoodsDescription = reader["Goods_description"],
                                PortOfDischarge = reader["Port_of_discharge"],
                                ArrivalDate = reader["Arrival_date"],
                                DeliverDate = reader["Deliver_date"],
                                Vessel = reader["Vessel"],
                                Voyage = reader["Voyage"],
                                ShipperSeal = reader["ShipperSeal"],
                                PortDischargeDate = reader["Port_discharge_date"],
                                GrossWeight = reader["Gross_weight"]
                                
                            };

                            return Ok(result);
                        }
                    }
                }
            }

            return NotFound();
        }


    }
}
