# chronofs

A FUSE filesystem built on technologies with point-in-time restore capabilities such as
[Litestream](https://litestream.io/) (and maybe CockroachDB in the future).

You can use chronofs to build ultra durable typically single-node systems. Chronofs in particular
targets Minecraft, allowing you to build Minecraft servers with the ability to perform
point-in-time restores with extremely high durability via state management in S3-like services
with Litestream. You can also take advantage of remote state management to run self-healing
Minecraft servers with no more than a few minutes of rollback during crashes.
