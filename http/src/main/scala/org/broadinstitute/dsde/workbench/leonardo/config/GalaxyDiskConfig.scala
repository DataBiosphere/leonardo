package org.broadinstitute.dsde.workbench.leonardo.config

import org.broadinstitute.dsde.workbench.leonardo.{BlockSize, DiskSize}

final case class GalaxyDiskConfig(nfsPersistenceName: String,
                                  postgresPersistenceName: String,
                                  // TODO: remove post-alpha once persistence is in place
                                  postgresDiskNameSuffix: String,
                                  postgresDiskSizeGB: DiskSize,
                                  postgresDiskBlockSize: BlockSize) {}
