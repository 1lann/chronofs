# how saving works
minecraft writes chunk data first, and then updates the header
that means after the header is updated, we have a good safe state to checkpoint
