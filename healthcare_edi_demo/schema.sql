-- =============================================================================
-- schema.sql
-- Healthcare EDI Analytics Warehouse — Azure SQL Database
-- Schemas: dim (dimensions) | fact (measures)
-- =============================================================================

-- ---------------------------------------------------------------------------
-- Drop existing tables (fact first to respect FK constraints)
-- ---------------------------------------------------------------------------
IF OBJECT_ID('fact.Payments',        'U') IS NOT NULL DROP TABLE fact.Payments
GO
IF OBJECT_ID('fact.Acknowledgments', 'U') IS NOT NULL DROP TABLE fact.Acknowledgments
GO
IF OBJECT_ID('fact.ClaimLines',      'U') IS NOT NULL DROP TABLE fact.ClaimLines
GO
IF OBJECT_ID('fact.Claims',          'U') IS NOT NULL DROP TABLE fact.Claims
GO
IF OBJECT_ID('dim.Patient',          'U') IS NOT NULL DROP TABLE dim.Patient
GO
IF OBJECT_ID('dim.Provider',         'U') IS NOT NULL DROP TABLE dim.Provider
GO
IF OBJECT_ID('dim.ProcedureCode',    'U') IS NOT NULL DROP TABLE dim.ProcedureCode
GO
IF OBJECT_ID('dim.DiagnosisCode',    'U') IS NOT NULL DROP TABLE dim.DiagnosisCode
GO
IF OBJECT_ID('dim.Date',             'U') IS NOT NULL DROP TABLE dim.Date
GO
IF OBJECT_ID('dim.Location',         'U') IS NOT NULL DROP TABLE dim.Location
GO
IF OBJECT_ID('dim.Payer',            'U') IS NOT NULL DROP TABLE dim.Payer
GO

-- ---------------------------------------------------------------------------
-- Schemas
-- ---------------------------------------------------------------------------
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'dim')
    EXEC('CREATE SCHEMA dim')
GO

IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'fact')
    EXEC('CREATE SCHEMA fact')
GO

-- =============================================================================
-- DIMENSION TABLES
-- =============================================================================

-- ---------------------------------------------------------------------------
-- dim.Date  (date spine, DateKey = YYYYMMDD integer)
-- ---------------------------------------------------------------------------
IF OBJECT_ID('dim.Date', 'U') IS NULL
BEGIN
    CREATE TABLE dim.Date (
        DateKey         INT          NOT NULL,
        FullDate        DATE         NOT NULL,
        Year            SMALLINT     NOT NULL,
        Quarter         TINYINT      NOT NULL,
        Month           TINYINT      NOT NULL,
        MonthName       VARCHAR(20)  NOT NULL,
        Day             TINYINT      NOT NULL,
        DayName         VARCHAR(20)  NOT NULL,
        WeekOfYear      TINYINT      NOT NULL,
        IsWeekend       BIT          NOT NULL DEFAULT 0,
        IsHoliday       BIT          NOT NULL DEFAULT 0,
        CONSTRAINT PK_Date PRIMARY KEY (DateKey)
    )
END
GO

-- ---------------------------------------------------------------------------
-- dim.Location
-- ---------------------------------------------------------------------------
IF OBJECT_ID('dim.Location', 'U') IS NULL
BEGIN
    CREATE TABLE dim.Location (
        LocationKey     INT          IDENTITY(1,1) NOT NULL,
        LocationName    VARCHAR(100) NOT NULL,
        AddressLine1    VARCHAR(200) NULL,
        City            VARCHAR(100) NULL,
        State           CHAR(2)      NULL,
        ZipCode         VARCHAR(10)  NULL,
        Phone           VARCHAR(20)  NULL,
        TaxID           VARCHAR(20)  NULL,
        GroupNPI        VARCHAR(10)  NULL,
        CONSTRAINT PK_Location PRIMARY KEY (LocationKey)
    )
END
GO

-- ---------------------------------------------------------------------------
-- dim.Payer
-- ---------------------------------------------------------------------------
IF OBJECT_ID('dim.Payer', 'U') IS NULL
BEGIN
    CREATE TABLE dim.Payer (
        PayerKey            INT           IDENTITY(1,1) NOT NULL,
        PayerID             VARCHAR(20)   NOT NULL,
        PayerName           VARCHAR(200)  NOT NULL,
        PayerType           VARCHAR(50)   NOT NULL,
        PayerCategory       VARCHAR(50)   NOT NULL,
        AvgDaysToPayment    SMALLINT      NULL,
        DenialRate          DECIMAL(5,4)  NULL,
        CONSTRAINT PK_Payer PRIMARY KEY (PayerKey)
    )
END
GO

-- ---------------------------------------------------------------------------
-- dim.Provider
-- ---------------------------------------------------------------------------
IF OBJECT_ID('dim.Provider', 'U') IS NULL
BEGIN
    CREATE TABLE dim.Provider (
        ProviderKey     INT          IDENTITY(1,1) NOT NULL,
        ProviderNPI     VARCHAR(10)  NOT NULL,
        FirstName       VARCHAR(100) NOT NULL,
        LastName        VARCHAR(100) NOT NULL,
        Specialty       VARCHAR(100) NOT NULL,
        ProviderType    VARCHAR(10)  NOT NULL,   -- MD, NP, PA
        LocationKey     INT          NOT NULL,
        DEANumber       VARCHAR(20)  NULL,
        TaxID           VARCHAR(20)  NULL,
        CONSTRAINT PK_Provider PRIMARY KEY (ProviderKey),
        CONSTRAINT FK_Provider_Location
            FOREIGN KEY (LocationKey) REFERENCES dim.Location(LocationKey)
    )
END
GO

-- ---------------------------------------------------------------------------
-- dim.Patient
-- ---------------------------------------------------------------------------
IF OBJECT_ID('dim.Patient', 'U') IS NULL
BEGIN
    CREATE TABLE dim.Patient (
        PatientKey          INT          IDENTITY(1,1) NOT NULL,
        PatientID           VARCHAR(20)  NOT NULL,
        FirstName           VARCHAR(100) NOT NULL,
        LastName            VARCHAR(100) NOT NULL,
        DateOfBirth         DATE         NOT NULL,
        Gender              CHAR(1)      NOT NULL,
        AddressLine1        VARCHAR(200) NULL,
        City                VARCHAR(100) NULL,
        State               CHAR(2)      NULL,
        ZipCode             VARCHAR(10)  NULL,
        Phone               VARCHAR(20)  NULL,
        InsuranceMemberID   VARCHAR(30)  NULL,
        PrimaryPayerKey     INT          NULL,
        CONSTRAINT PK_Patient PRIMARY KEY (PatientKey),
        CONSTRAINT FK_Patient_Payer
            FOREIGN KEY (PrimaryPayerKey) REFERENCES dim.Payer(PayerKey)
    )
END
GO

-- ---------------------------------------------------------------------------
-- dim.ProcedureCode  (CPT)
-- ---------------------------------------------------------------------------
IF OBJECT_ID('dim.ProcedureCode', 'U') IS NULL
BEGIN
    CREATE TABLE dim.ProcedureCode (
        ProcedureCodeKey    INT            IDENTITY(1,1) NOT NULL,
        CPTCode             VARCHAR(10)    NOT NULL,
        Description         VARCHAR(500)   NOT NULL,
        Category            VARCHAR(100)   NOT NULL,
        BaseCharge          DECIMAL(10,2)  NULL,
        RVU                 DECIMAL(8,3)   NULL,
        CONSTRAINT PK_ProcedureCode PRIMARY KEY (ProcedureCodeKey)
    )
END
GO

-- ---------------------------------------------------------------------------
-- dim.DiagnosisCode  (ICD-10-CM)
-- ---------------------------------------------------------------------------
IF OBJECT_ID('dim.DiagnosisCode', 'U') IS NULL
BEGIN
    CREATE TABLE dim.DiagnosisCode (
        DiagnosisCodeKey    INT           IDENTITY(1,1) NOT NULL,
        ICD10Code           VARCHAR(10)   NOT NULL,
        Description         VARCHAR(500)  NOT NULL,
        Category            VARCHAR(100)  NOT NULL,
        CONSTRAINT PK_DiagnosisCode PRIMARY KEY (DiagnosisCodeKey)
    )
END
GO

-- =============================================================================
-- FACT TABLES
-- =============================================================================

-- ---------------------------------------------------------------------------
-- fact.Claims
-- ---------------------------------------------------------------------------
IF OBJECT_ID('fact.Claims', 'U') IS NULL
BEGIN
    CREATE TABLE fact.Claims (
        ClaimKey                INT            IDENTITY(1,1) NOT NULL,
        ClaimID                 VARCHAR(30)    NOT NULL,
        PatientKey              INT            NOT NULL,
        ProviderKey             INT            NOT NULL,
        LocationKey             INT            NOT NULL,
        PayerKey                INT            NOT NULL,
        ServiceDateKey          INT            NOT NULL,
        SubmissionDateKey       INT            NOT NULL,
        TransactionType         VARCHAR(10)    NOT NULL DEFAULT '837P',
        ClaimStatus             VARCHAR(50)    NOT NULL,
        EDIInterchangeID        VARCHAR(30)    NULL,
        FunctionalGroupID       VARCHAR(30)    NULL,
        TotalBilledAmount       DECIMAL(12,2)  NULL,
        TotalAllowedAmount      DECIMAL(12,2)  NULL,
        TotalPaidAmount         DECIMAL(12,2)  NULL,
        TotalAdjustmentAmount   DECIMAL(12,2)  NULL,
        PatientResponsibility   DECIMAL(12,2)  NULL,
        ClaimStatusCode         VARCHAR(10)    NULL,
        PriorAuthNumber         VARCHAR(30)    NULL,
        ReferralNumber          VARCHAR(30)    NULL,
        CreatedAt               DATETIME2      DEFAULT GETDATE(),
        CONSTRAINT PK_Claims PRIMARY KEY (ClaimKey),
        CONSTRAINT UQ_Claims_ClaimID UNIQUE (ClaimID),
        CONSTRAINT FK_Claims_Patient
            FOREIGN KEY (PatientKey)    REFERENCES dim.Patient(PatientKey),
        CONSTRAINT FK_Claims_Provider
            FOREIGN KEY (ProviderKey)   REFERENCES dim.Provider(ProviderKey),
        CONSTRAINT FK_Claims_Location
            FOREIGN KEY (LocationKey)   REFERENCES dim.Location(LocationKey),
        CONSTRAINT FK_Claims_Payer
            FOREIGN KEY (PayerKey)      REFERENCES dim.Payer(PayerKey),
        CONSTRAINT FK_Claims_ServiceDate
            FOREIGN KEY (ServiceDateKey)    REFERENCES dim.Date(DateKey),
        CONSTRAINT FK_Claims_SubmissionDate
            FOREIGN KEY (SubmissionDateKey) REFERENCES dim.Date(DateKey)
    )
END
GO

-- ---------------------------------------------------------------------------
-- fact.ClaimLines
-- ---------------------------------------------------------------------------
IF OBJECT_ID('fact.ClaimLines', 'U') IS NULL
BEGIN
    CREATE TABLE fact.ClaimLines (
        ClaimLineKey        INT            IDENTITY(1,1) NOT NULL,
        ClaimKey            INT            NOT NULL,
        LineNumber          TINYINT        NOT NULL,
        ProcedureCodeKey    INT            NOT NULL,
        DiagnosisCodeKey    INT            NOT NULL,
        ServiceDateKey      INT            NOT NULL,
        Units               TINYINT        NOT NULL DEFAULT 1,
        ChargeAmount        DECIMAL(10,2)  NULL,
        AllowedAmount       DECIMAL(10,2)  NULL,
        PaidAmount          DECIMAL(10,2)  NULL,
        AdjustmentAmount    DECIMAL(10,2)  NULL,
        Modifier1           VARCHAR(5)     NULL,
        Modifier2           VARCHAR(5)     NULL,
        RevenueCode         VARCHAR(10)    NULL,
        CONSTRAINT PK_ClaimLines PRIMARY KEY (ClaimLineKey),
        CONSTRAINT FK_ClaimLines_Claim
            FOREIGN KEY (ClaimKey)          REFERENCES fact.Claims(ClaimKey),
        CONSTRAINT FK_ClaimLines_ProcCode
            FOREIGN KEY (ProcedureCodeKey)  REFERENCES dim.ProcedureCode(ProcedureCodeKey),
        CONSTRAINT FK_ClaimLines_DiagCode
            FOREIGN KEY (DiagnosisCodeKey)  REFERENCES dim.DiagnosisCode(DiagnosisCodeKey),
        CONSTRAINT FK_ClaimLines_ServiceDate
            FOREIGN KEY (ServiceDateKey)    REFERENCES dim.Date(DateKey)
    )
END
GO

-- ---------------------------------------------------------------------------
-- fact.Acknowledgments  (999 + 277CA)
-- ---------------------------------------------------------------------------
IF OBJECT_ID('fact.Acknowledgments', 'U') IS NULL
BEGIN
    CREATE TABLE fact.Acknowledgments (
        AcknowledgmentKey               INT           IDENTITY(1,1) NOT NULL,
        ClaimKey                        INT           NOT NULL,
        TransactionType                 VARCHAR(10)   NOT NULL,   -- '999' or '277CA'
        AcknowledgmentDateKey           INT           NOT NULL,
        AcknowledgmentDate              DATE          NOT NULL,
        Status                          VARCHAR(20)   NOT NULL,   -- 'Accepted' | 'Rejected'
        RejectionReasonCode             VARCHAR(20)   NULL,
        RejectionReasonDescription      VARCHAR(500)  NULL,
        InterchangeControlNumber        VARCHAR(20)   NULL,
        FunctionalGroupControlNumber    VARCHAR(20)   NULL,
        TransactionSetControlNumber     VARCHAR(20)   NULL,
        CONSTRAINT PK_Acknowledgments PRIMARY KEY (AcknowledgmentKey),
        CONSTRAINT FK_Acks_Claim
            FOREIGN KEY (ClaimKey)              REFERENCES fact.Claims(ClaimKey),
        CONSTRAINT FK_Acks_Date
            FOREIGN KEY (AcknowledgmentDateKey) REFERENCES dim.Date(DateKey)
    )
END
GO

-- ---------------------------------------------------------------------------
-- fact.Payments  (835 Remittance Advice)
-- ---------------------------------------------------------------------------
IF OBJECT_ID('fact.Payments', 'U') IS NULL
BEGIN
    CREATE TABLE fact.Payments (
        PaymentKey              INT            IDENTITY(1,1) NOT NULL,
        ClaimKey                INT            NOT NULL,
        RemittanceNumber        VARCHAR(30)    NOT NULL,
        PaymentDateKey          INT            NOT NULL,
        PaymentDate             DATE           NOT NULL,
        BilledAmount            DECIMAL(12,2)  NULL,
        AllowedAmount           DECIMAL(12,2)  NULL,
        PaidAmount              DECIMAL(12,2)  NULL,
        AdjustmentAmount        DECIMAL(12,2)  NULL,
        PatientResponsibility   DECIMAL(12,2)  NULL,
        CARC                    VARCHAR(10)    NULL,
        RARC                    VARCHAR(10)    NULL,
        DenialCategory          VARCHAR(100)   NULL,
        DaysToPayment           SMALLINT       NULL,
        PaymentMethod           VARCHAR(10)    NULL,   -- 'EFT' | 'CHK'
        CheckEFTNumber          VARCHAR(30)    NULL,
        CONSTRAINT PK_Payments PRIMARY KEY (PaymentKey),
        CONSTRAINT FK_Payments_Claim
            FOREIGN KEY (ClaimKey)      REFERENCES fact.Claims(ClaimKey),
        CONSTRAINT FK_Payments_Date
            FOREIGN KEY (PaymentDateKey) REFERENCES dim.Date(DateKey)
    )
END
GO

-- =============================================================================
-- INDEXES  (Power BI Import mode performance)
-- =============================================================================

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IX_Claims_PayerKey')
    CREATE INDEX IX_Claims_PayerKey       ON fact.Claims(PayerKey)
GO
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IX_Claims_ProviderKey')
    CREATE INDEX IX_Claims_ProviderKey    ON fact.Claims(ProviderKey)
GO
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IX_Claims_ServiceDateKey')
    CREATE INDEX IX_Claims_ServiceDateKey ON fact.Claims(ServiceDateKey)
GO
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IX_Claims_ClaimStatus')
    CREATE INDEX IX_Claims_ClaimStatus    ON fact.Claims(ClaimStatus)
GO
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IX_ClaimLines_ClaimKey')
    CREATE INDEX IX_ClaimLines_ClaimKey   ON fact.ClaimLines(ClaimKey)
GO
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IX_Payments_ClaimKey')
    CREATE INDEX IX_Payments_ClaimKey     ON fact.Payments(ClaimKey)
GO
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IX_Acks_ClaimKey')
    CREATE INDEX IX_Acks_ClaimKey         ON fact.Acknowledgments(ClaimKey)
GO
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IX_Acks_TransactionType')
    CREATE INDEX IX_Acks_TransactionType  ON fact.Acknowledgments(TransactionType)
GO
