package org.broadinstitute.dsde.workbench.leonardo
package config

final case class GalaxyDiskConfig(nfsPersistenceName: String,
                                  nfsMinimumDiskSizeGB: DiskSize,
                                  postgresPersistenceName: String,
                                  postgresDiskNameSuffix: String,
                                  postgresDiskSizeGB: DiskSize,
                                  postgresDiskBlockSize: BlockSize)
