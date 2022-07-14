package path

import "path"

type Path string

func (p Path) Parent() Path { return Path(path.Dir(string(p))) }

func (p Path) Child(child string) Path { return Path(path.Join(string(p), child)) }

func (p Path)