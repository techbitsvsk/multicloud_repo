// ─────────────────────────────────────────────────────────────────────────────
// bicep/azure.bicep – Storage account, containers, Entra groups, RBAC
//
// Deploy:
//   az deployment group create \
//     --resource-group rg-iceberg-pipeline \
//     --template-file azure.bicep \
//     --parameters storageAccountName=sticebergpipeline
//
// NOTE: Fabric workspace & Lakehouse creation is NOT automatable via Bicep
// today.  Manual steps are documented in the runbook.
// ─────────────────────────────────────────────────────────────────────────────

@description('Name of the storage account (ADLS Gen2)')
param storageAccountName string

@description('Azure region')
param location string = resourceGroup().location

@description('Tags')
param tags object = {
  Project: 'iceberg-pipeline'
  Environment: 'dev'
  ManagedBy: 'bicep'
}

// ── Storage Account (ADLS Gen2) ─────────────────────────────────────────────

resource storageAccount 'Microsoft.Storage/storageAccounts@2023-01-01' = {
  name: storageAccountName
  location: location
  tags: tags
  kind: 'StorageV2'
  sku: { name: 'Standard_LRS' }
  properties: {
    isHnsEnabled: true                          // Hierarchical namespace = ADLS Gen2
    minimumTlsVersion: 'TLS1_2'
    supportsHttpsTrafficOnly: true
    allowBlobPublicAccess: false
    encryption: {
      services: {
        blob: { enabled: true, keyType: 'Account' }
        file: { enabled: true, keyType: 'Account' }
      }
      keySource: 'Microsoft.Storage'            // Use CMK via Key Vault in prod
    }
    networkAcls: {
      defaultAction: 'Deny'                     // Locked down; add VNet rules
      bypass: 'AzureServices'
    }
  }
}

resource blobService 'Microsoft.Storage/storageAccounts/blobServices@2023-01-01' = {
  parent: storageAccount
  name: 'default'
}

// Containers
resource rawContainer 'Microsoft.Storage/storageAccounts/blobServices/containers@2023-01-01' = {
  parent: blobService
  name: 'raw'
  properties: { publicAccess: 'None' }
}

resource icebergContainer 'Microsoft.Storage/storageAccounts/blobServices/containers@2023-01-01' = {
  parent: blobService
  name: 'iceberg'
  properties: { publicAccess: 'None' }
}

// ── RBAC Role Assignments ───────────────────────────────────────────────────
// Storage Blob Data Contributor = ba92f5b4-2d11-453d-a403-e96b0029c9fe
// Storage Blob Data Reader      = 2a2b9908-6ea1-4ae2-8e65-a410df84e7d1

// These require Entra group object IDs as parameters.
// Create groups first:
//   az ad group create --display-name grp-data-engineer --mail-nickname grp-data-engineer
//   az ad group create --display-name grp-data-analyst  --mail-nickname grp-data-analyst
//   az ad group create --display-name grp-platform-admin --mail-nickname grp-platform-admin

@description('Object ID of the grp-data-engineer Entra group')
param dataEngineerGroupId string = ''

@description('Object ID of the grp-data-analyst Entra group')
param dataAnalystGroupId string = ''

@description('Object ID of the grp-platform-admin Entra group')
param platformAdminGroupId string = ''

var storageBlobContributor = 'ba92f5b4-2d11-453d-a403-e96b0029c9fe'
var storageBlobReader      = '2a2b9908-6ea1-4ae2-8e65-a410df84e7d1'

// Engineer: Contributor (read + write) on iceberg container
resource engineerRbac 'Microsoft.Authorization/roleAssignments@2022-04-01' = if (!empty(dataEngineerGroupId)) {
  name: guid(storageAccount.id, dataEngineerGroupId, storageBlobContributor)
  scope: storageAccount
  properties: {
    principalId: dataEngineerGroupId
    roleDefinitionId: subscriptionResourceId('Microsoft.Authorization/roleDefinitions', storageBlobContributor)
    principalType: 'Group'
  }
}

// Analyst: Reader (read-only) on iceberg container
resource analystRbac 'Microsoft.Authorization/roleAssignments@2022-04-01' = if (!empty(dataAnalystGroupId)) {
  name: guid(storageAccount.id, dataAnalystGroupId, storageBlobReader)
  scope: storageAccount
  properties: {
    principalId: dataAnalystGroupId
    roleDefinitionId: subscriptionResourceId('Microsoft.Authorization/roleDefinitions', storageBlobReader)
    principalType: 'Group'
  }
}

// Platform admin: Contributor (full)
resource adminRbac 'Microsoft.Authorization/roleAssignments@2022-04-01' = if (!empty(platformAdminGroupId)) {
  name: guid(storageAccount.id, platformAdminGroupId, storageBlobContributor)
  scope: storageAccount
  properties: {
    principalId: platformAdminGroupId
    roleDefinitionId: subscriptionResourceId('Microsoft.Authorization/roleDefinitions', storageBlobContributor)
    principalType: 'Group'
  }
}

// ── Outputs ─────────────────────────────────────────────────────────────────

output storageAccountId string = storageAccount.id
output rawContainerPath string = 'abfss://raw@${storageAccountName}.dfs.core.windows.net/'
output icebergWarehousePath string = 'abfss://iceberg@${storageAccountName}.dfs.core.windows.net/iceberg/'
