import React from 'react';

import { Button } from './Button';
import './header.css';

export interface HeaderProps {
  logoUrl: string;
  title: string;
  fileName: string;
  onSelect?: () => void;
}

export const Header = ({ logoUrl, title, fileName, onSelect }: HeaderProps) => (
  <header>
    <div className="storybook-header">
      <div>
      <img src={logoUrl} className="header-logo"/>
        <h1>{title}</h1>
        <div className="header-file-name">{fileName}</div>
      </div>
      <div>
        <Button size="small" label="Select Dataset" onClick={onSelect}/>
      </div>
    </div>
  </header>
);