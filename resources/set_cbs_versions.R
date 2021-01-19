# update lockfile versions to cbs versions

library(jsonlite)
pck <- read.csv("resources/r_packages_cbs.csv", stringsAsFactors = FALSE)
lck <- read_json("renv.lock")

for (p in names(lck$Packages)) {
  lpack <- lck$Packages[[p]]
  lck_version <- lpack$Version
  
  if (!p %in% pck$Package) {
    if (!p == "renv") {
      cat("!!! [", p, "] :", lck_version, "=> *\n")
      lck$Packages[[p]] <- NULL
    }
  } else {
    cbs_version <- pck[pck$Package == p, "Version"][1]
    if (lck_version == cbs_version) next
    
    cat("[", p, "] :", lck_version, "=>", cbs_version, "\n")
    
    lck$Packages[[p]] <- list(
      "Package"    = p,
      "Version"    = cbs_version,
      "Source"     = "Repository",
      "Repository" = "CRAN"
    )
  }
}

write_json(lck, "renv.lock", flatten = TRUE, pretty = TRUE, auto_unbox = TRUE)

cat("\nNow run the following line to get your packages in the right version:
    
    renv::restore(clean = TRUE)\n\n")
