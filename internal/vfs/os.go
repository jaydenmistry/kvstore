package vfs

import "os"

// OS is the real filesystem.
type OS struct{}

var _ FS = OS{}

func (OS) OpenFile(name string, flag int, perm os.FileMode) (File, error) {
	f, err := os.OpenFile(name, flag, perm)
	if err != nil {
		return nil, err
	}
	return osFile{f}, nil
}

func (OS) SyncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	if err := d.Sync(); err != nil {
		d.Close()
		return err
	}
	return d.Close()
}

func (OS) Rename(oldpath, newpath string) error { return os.Rename(oldpath, newpath) }

func (OS) Remove(name string) error { return os.Remove(name) }

type osFile struct{ *os.File }

func (f osFile) Size() (int64, error) {
	fi, err := f.Stat()
	if err != nil {
		return 0, err
	}
	return fi.Size(), nil
}
