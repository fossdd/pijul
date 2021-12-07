# Pijul

This is the repository of Pijul, a sound and fast distributed version control system based on a mathematical theory of asynchronous work.

## License

The license is GPL-2.0.

## Documentation

While we are working on documentation, here are a few useful commands:

### Create a repository

~~~
$ pijul init
~~~

### Add files

If you want to track all the files in a folder, and record that change, do:

~~~
$ pijul rec that_folder
~~~

If you want to add files to track:

~~~
$ pijul add these_files
~~~


### Make a change

Pijul is based on changes, so perhaps the most important command is the one that creates them:

~~~
$ pijul rec
~~~

You will be presented with a change draft, which you can approve or edit by deleting sections, where sections are introduced by a number (of the form `1.`) followed by the name of an operation (example: `File addition: "my_file" in "/" 420`).

You can of course try other edits, but they are not guaranteed to work.

### Collaborate

A *remote* is the reference to another repository, for example `pijul@ssh.pijul.com:manual` for the manual repository, or `me@ssh.pijul.com:pijul/manual`, `https://nest.pijul.com/pijul/manual`, or a local path `/path/to/my/repository`.

The `remote` command allows one to view the saved remotes and possibly delete them.

The `push` and `pull` commands exchange changes with remotes.

Cloning repositories need a target directory at the moment, or else take the current directory as the target:

~~~
$ pijul clone https://nest.pijul.com/pijul/pijul
~~~

Hint: clones over SSH are almost always faster.

### Going back in time

If you want to reset your files to the last recorded version, just do:

~~~
$ pijul reset
~~~

If you want to remove some changes from the history:

~~~
$ pijul unrecord PREFIX_OF_CHANGE_HASH
~~~

Where `PREFIX_OF_CHANGE_HASH` is an unambiguous prefix of a change hash, which can be found by doing `pijul log`.


### Import a Git repository

If you have compiled Pijul with `--features git`, the `git` command allows one to import changes from a Git repository. This works by replaying the repository history, and is not particularly optimised, hence it may be take a long time on large repositories.

One missing feature of Git at the moment is symbolic links, which are treated as regular files by that command (i.e. the same might get imported multiple times).

### About channels

Channels are a way to maintain two related versions of a repository in the same place (a bit like branches in Git).

Formally, a channel is a pointer to a set of changes (the *state* of a channel is a set of changes).

However, channels are different from Git branches, and do not serve the same purpose. In Pijul, **independent changes commute**, which means that in many cases where branches are used in Git, there is no need to create a channel in Pijul.

The main differences with Git branches are:

- The identity of a change doesn't depend on the branch it is on, or in other words, rebase and merge are the same operation in Pijul.

- This implies that conflicts do not mysteriously come back after you solve them (which is what `git rerere` is for).

- Also, conflicts are between changes, so the resolution of a conflict on one channel solves the same conflict in all other channels.


## Contributing

We welcome all contributions. Moreover, as this projects aims at making it easier to collaborate with others (we're getting there), we obviously value mutual respect and inclusiveness above anything else.

Moreover, since this is a Rust project, we ask contributors to run `cargo fmt` on their code before recording changes. This can be done automatically by adding the following lines to the repository's `.pijul/config`:

```
[hooks]
record = [ "cargo fmt" ]
```
